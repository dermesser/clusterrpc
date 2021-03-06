package client

import (
	"github.com/dermesser/clusterrpc/proto"
	golog "log"
	"time"

	pb "github.com/gogo/protobuf/proto"
)

// A client contains a channel and some metadata, a state machine, and a stack of client filters.
type Client struct {
	channel RpcChannel
	name    string

	active bool
	// To prevent hassle with the REQ state machine, we implement a similar
	// one ourselves; as long as request_active == true, no new requests
	// can be created
	request_active chan bool

	defaultParams RequestParams

	last_sent time.Time
	rpclogger *golog.Logger

	filters []ClientFilter
}

// NewClient is deprecated; use New()
func NewClient(name string, channel *RpcChannel) Client {
	return New(name, channel)
}

// Creates a new client from the channel.
// Don't share a channel among two concurrently active clients.
func New(name string, channel *RpcChannel) Client {
	rqa := make(chan bool, 1)
	rqa <- true
	return Client{name: name, channel: *channel, active: true, request_active: rqa, defaultParams: *NewParams(), filters: default_filters}
}

// Set socket timeout (default 10s) and whether to propagate this timeout through the call tree.
func (client *Client) SetTimeout(d time.Duration, propagate bool) {
	if !client.active {
		return
	}

	client.defaultParams.timeout = d
	client.defaultParams.deadline_propagation = propagate
	client.channel.SetTimeout(d)
}

// Disconnects the channel and disables the client
func (client *Client) Destroy() {
	client.channel.destroy()
	client.channel = RpcChannel{}
	client.active = false
}

// Create a Request to be sent by this client. If a previous request has not
// been finished, this method returns nil!
func (client *Client) NewRequest(service, endpoint string) *Request {
	return &Request{client: client, params: client.defaultParams, service: service, endpoint: endpoint}
}

// Sends a request to the server, asking whether it accepts requests and
// testing general connectivity. Uses a timeout of 1 second.
func (client *Client) IsHealthy() bool {
	return client.IsHealthyWithin(1 * time.Second)
}

// Same as IsHealthy(), but with a configurable timeout
func (client *Client) IsHealthyWithin(d time.Duration) bool {
	rp := client.NewRequest("__CLUSTERRPC", "Health").SetParameters(NewParams().Timeout(d)).Go([]byte{})
	return rp.Ok()
}

// Oneshot-API: Send a request with raw data to the connected RPC server.
func (cl *Client) Request(data []byte, service, endpoint string, trace_dest *proto.TraceInfo) ([]byte, error) {
	rp := cl.NewRequest(service, endpoint).SetTrace(trace_dest).Go(data)

	if !rp.Ok() {
		return nil, &rp
	}
	return rp.Payload(), nil
}

// Oneshot-API: Send a request with the given protocol buffers to the connected RPC server.
func (cl *Client) RequestProtobuf(request, reply pb.Message, service, endpoint string, trace_dest *proto.TraceInfo) error {
	rp := cl.NewRequest(service, endpoint).SetTrace(trace_dest).GoProto(request)

	if !rp.Ok() {
		return &rp
	}
	rp.GetResponseMessage(reply)
	return nil
}
