package client

import (
	"errors"
	"fmt"
	"time"

	"github.com/dermesser/clusterrpc/log"
	smgr "github.com/dermesser/clusterrpc/securitymanager"
	zmq "github.com/pebbe/zmq4"
)

// A TCP/IP or Unix socket address
type PeerAddress struct {
	host string
	port uint

	path string
}

// Construct a new peer address.
func Peer(host string, port uint) PeerAddress {
	return PeerAddress{host: host, port: port}
}

// Construct a peer address for a unix socket peer.
func IPCPeer(path string) PeerAddress {
	return PeerAddress{path: path}
}

// Convert a PeerAddress to a ZeroMQ URL.
func (pa *PeerAddress) ToUrl() string {
	if pa.host != "" {
		return fmt.Sprintf("tcp://%s:%d", pa.host, pa.port)
	} else if pa.path != "" {
		return fmt.Sprintf("ipc://%s", pa.path)
	} else {
		return ""
	}
}

func (pa *PeerAddress) toDebugStr() string {
	if pa.host != "" {
		return fmt.Sprintf("tcp:%s:%d", pa.host, pa.port)
	} else if pa.path != "" {
		return fmt.Sprintf("unix:%s", pa.path)
	} else {
		return ""
	}
}

func (pa *PeerAddress) String() string {
	return pa.toDebugStr()
}
func (pa *PeerAddress) GoString() string {
	return pa.toDebugStr()
}

func (pa *PeerAddress) equals(pa2 PeerAddress) bool {
	return (pa.host != "" && pa.host == pa2.host && pa.port == pa2.port) || (pa.path != "" && pa.path == pa2.path)
}

// A channel to an RPC server. It is threadsafe, but should not be shared among multiple clients.
//
// TODO(lbo): Think about implementing a channel on top of DEALER, with a
// background goroutine delivering results to waiting requests.
//
// The protocol here looks as follows: [request ID, client ID, "", payload].
// It is (currently) fully managed by ZeroMQ.
type RpcChannel struct {
	channel *zmq.Socket
	timeout time.Duration
	// Slices to allow multiple connections (round-robin)
	peers []PeerAddress

	// Parallel request handling
	stop     chan bool
	clientId []byte
	inFlight map[string]chan clientResp
}

// Create a new RpcChannel.
// security_manager may be nil.
func NewRpcChannel(security_manager *smgr.ClientSecurityManager) (*RpcChannel, error) {
	channel := RpcChannel{}

	var err error
	channel.channel, err = zmq.NewSocket(zmq.DEALER)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when creating Req socket:", err.Error())
		return nil, err
	}

	if security_manager != nil {
		err = security_manager.ApplyToClientSocket(channel.channel)

		if err != nil {
			log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when setting up security:", err.Error())
			return nil, err
		}
	}

	channel.channel.SetIpv6(true)
	channel.channel.SetLinger(0)
	channel.channel.SetReconnectIvl(100 * time.Millisecond)
	channel.channel.SetImmediate(true)

	channel.channel.SetSndtimeo(10 * time.Second)
	channel.channel.SetRcvtimeo(10 * time.Second)
	//channel.channel.SetReqRelaxed(1)
	//channel.channel.SetReqCorrelate(1)

	channel.clientId = []byte(log.GetLogToken())
	channel.inFlight = map[string]chan clientResp{}

	go channel.backgroundDispatcher()

	return &channel, nil
}

// NewChannelAndConnect creates a new channel and connects it to `addr`.
func NewChannelAndConnect(addr PeerAddress, security_manager *smgr.ClientSecurityManager) (*RpcChannel, error) {
	channel, err := NewRpcChannel(security_manager)

	if err != nil {
		return nil, err
	}

	return channel, channel.Connect(addr)
}

// Connect channel to adr.
// (This adds the server to the set of connections of this channel; connections are used in a round-robin fashion)
func (c *RpcChannel) Connect(addr PeerAddress) error {
	peer := addr.ToUrl()
	c.channel.Disconnect(peer)
	err := c.channel.Connect(peer)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Could not establish connection to single peer;",
			err.Error(), fmt.Sprintf("tcp://%s:%d", addr.host, addr.port))
		return err
	}
	c.peers = append(c.peers, addr)
	return nil
}

// Disconnect the given peer (i.e., take it out of the connection pool)
func (c *RpcChannel) Disconnect(peer PeerAddress) {
	for j := range c.peers {
		if peer.equals(c.peers[j]) {
			c.channel.Disconnect(peer.ToUrl())
			c.peers = append(c.peers[0:j], c.peers[j+1:]...)
			break
		}
	}
}

// First disconnect, then reconnect to all peers.
func (c *RpcChannel) Reconnect() {
	peers := make([]PeerAddress, len(c.peers))
	copy(peers, c.peers)
	for _, p := range peers {
		c.Disconnect(p)
	}
	for _, p := range peers {
		c.Connect(p)
	}
}

// Set send/receive timeout on this channel.
func (c *RpcChannel) SetTimeout(d time.Duration) {
	c.timeout = d
	c.channel.SetSndtimeo(d)
	c.channel.SetRcvtimeo(d)
}

func (c *RpcChannel) destroy() {
	c.channel.Close()
}

// Sent by the backgroundDispatcher() goroutine to any waiting clients.
type clientResp struct {
	resp []byte
	err  error
}

// Dispatch incoming responses to clients
func (c *RpcChannel) backgroundDispatcher() {
	for {
		frames, err := c.channel.RecvMessageBytes(0)
		log.CRPC_log(log.LOGLEVEL_INFO, "received:", frames, err)
		if err != nil {
			for _, ch := range c.inFlight {
				ch <- clientResp{err: err}
			}
			c.inFlight = map[string]chan clientResp{}
			continue
		}
		ch := c.inFlight[string(frames[0])]
		if ch == nil {
			log.CRPC_log(log.LOGLEVEL_ERRORS, "Client not found!")
		}
		ch <- clientResp{resp: frames[2]}
	}
}

// Send a message, returning a unique request ID and an error.
func (c *RpcChannel) sendMessage(rqId string, request []byte) error {
	ch := make(chan clientResp, 1)
	c.inFlight[rqId] = ch
	log.CRPC_log(log.LOGLEVEL_INFO, "sending:", rqId, "", request)
	_, err := c.channel.SendMessage(rqId, "", request)
	return err
}

// Wait for a response with request ID rqId.
func (c *RpcChannel) receiveMessage(rqId string) ([]byte, error) {
	timeout := time.NewTimer(c.timeout)
	select {
	case resp := <-c.inFlight[rqId]:
		timeout.Stop()
		log.CRPC_log(log.LOGLEVEL_INFO, "response for client:", rqId, resp)
		delete(c.inFlight, rqId)
		return resp.resp, resp.err
	case <-timeout.C:
		return nil, errors.New("timeout expired while receiving")
	}
}
