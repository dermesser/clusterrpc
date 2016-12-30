package client

import (
	"clusterrpc/log"
	smgr "clusterrpc/securitymanager"
	"fmt"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// A TCP/IP address
type PeerAddress struct {
	host string
	port uint

	path string
}

// Construct a new peer address.
func Peer(host string, port uint) PeerAddress {
	return PeerAddress{host: host, port: port}
}

func IPCPeer(path string) PeerAddress {
	return PeerAddress{path: path}
}

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
		return fmt.Sprintf("%s:%d", pa.host, pa.port)
	} else if pa.path != "" {
		return fmt.Sprintf("%s", pa.path)
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
// TODO(lbo): Think about implementing a channel on top of DEALER, with a background goroutine delivering results to waiting requests.
type RpcChannel struct {
	channel *zmq.Socket

	// Slices to allow multiple connections (round-robin)
	peers []PeerAddress
}

// Create a new RpcChannel.
// security_manager may be nil.
func NewRpcChannel(security_manager *smgr.ClientSecurityManager) (*RpcChannel, error) {
	channel := RpcChannel{}

	var err error
	channel.channel, err = zmq.NewSocket(zmq.REQ)

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
	channel.channel.SetReqRelaxed(1)
	channel.channel.SetReqCorrelate(1)

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
			err.Error, fmt.Sprintf("tcp://%s:%d", addr.host, addr.port))
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

func (c *RpcChannel) SetTimeout(d time.Duration) {
	c.channel.SetSndtimeo(d)
	c.channel.SetRcvtimeo(d)
}

func (c *RpcChannel) destroy() {
	c.channel.Close()
}

func (c *RpcChannel) sendMessage(request []byte) error {
	_, err := c.channel.SendBytes(request, 0)
	return err
}

func (c *RpcChannel) receiveMessage() ([]byte, error) {
	msg, err := c.channel.RecvBytes(0)
	return msg, err
}
