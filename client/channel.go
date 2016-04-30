package client

import (
	"clusterrpc/log"
	"clusterrpc/proto"
	smgr "clusterrpc/securitymanager"
	"fmt"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// A TCP/IP address
type PeerAddress struct {
	host string
	port uint32
}

// Construct a new peer address.
func Peer(host string, port uint32) PeerAddress {
	return PeerAddress{host: host, port: port}
}

func (pa *PeerAddress) toUrl() string {
	return fmt.Sprintf("tcp://%s:%d", pa.host, pa.port)
}

func (pa *PeerAddress) equals(pa2 PeerAddress) bool {
	return pa.host == pa2.host && pa.port == pa2.port
}

// A channel to an RPC server.
type RpcChannel struct {
	sync.Mutex
	channel *zmq.Socket

	// Slices to allow multiple connections (round-robin)
	peers []PeerAddress

	security_manager *smgr.ClientSecurityManager
}

// Create a new RpcChannel.
// security_manager may be nil.
func NewChannel(security_manager *smgr.ClientSecurityManager) (*RpcChannel, error) {
	channel := RpcChannel{}

	var err error
	channel.channel, err = zmq.NewSocket(zmq.REQ)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when creating Req socket:", err.Error())
		return nil, &RequestError{status: proto.RPCResponse_STATUS_CLIENT_NETWORK_ERROR, err: err}
	}

	if security_manager != nil {
		err = security_manager.ApplyToClientSocket(channel.channel)

		if err != nil {
			log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when setting up security:", err.Error())
			return nil, &RequestError{status: proto.RPCResponse_STATUS_CLIENT_NETWORK_ERROR, err: err}
		}
	}

	channel.channel.SetIpv6(true)
	channel.channel.SetReconnectIvl(100 * time.Millisecond)

	channel.channel.SetSndtimeo(10 * time.Second)
	channel.channel.SetRcvtimeo(10 * time.Second)
	channel.channel.SetReqRelaxed(1)
	channel.channel.SetReqCorrelate(1)

	return &channel, nil
}

// Connect channel to adr.
// (This adds the server to the set of connections of this channel; connections are used in a round-robin fashion)
func (c *RpcChannel) Connect(addr PeerAddress) error {
	c.Lock()
	defer c.Unlock()

	peer := addr.toUrl()
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

func (c *RpcChannel) Disconnect(peer PeerAddress) {
	c.Lock()
	defer c.Unlock()

	for j := range c.peers {
		if peer.equals(c.peers[j]) {
			c.channel.Disconnect(peer.toUrl())
			c.peers = append(c.peers[0:j], c.peers[j:]...)
			break
		}
	}
}

func (c *RpcChannel) SetDeadline(d time.Duration) {
	c.Lock()
	defer c.Unlock()

	c.channel.SetSndtimeo(d)
	c.channel.SetRcvtimeo(d)
}

func (c *RpcChannel) destroy() {
	c.Lock()
	defer c.Unlock()

	c.channel.SetLinger(0)
	c.channel.Close()
}

func (c *RpcChannel) sendMessage(request []byte) error {
	c.Lock()
	defer c.Unlock()

	_, err := c.channel.SendBytes(request, 0)

	return err
}

func (c *RpcChannel) receiveMessage() ([]byte, error) {

	msg, err := c.channel.RecvBytes(0)

	return msg, err
}
