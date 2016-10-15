package client

import (
	"clusterrpc/proto"
	golog "log"
	"time"

	pb "github.com/gogo/protobuf/proto"
)

// A (new) client object. It contains a channel
type Client struct {
	channel RpcChannel
	name    string

	active bool
	// To prevent hassle with the REQ state machine, we implement a similar one ourselves; as long as request_active == true, no new requests can be created
	request_active bool

	default_params RequestParams

	last_sent time.Time
	rpclogger *golog.Logger

	filters []ClientFilter
}

// Creates a new client from the channel.
// Don't share a channel among two concurrently active clients.
func NewClient(name string, channel *RpcChannel) Client {
	return Client{name: name, channel: *channel, active: true, default_params: *NewParams(), filters: default_filters}
}

// Set socket timeout (default 10s) and whether to propagate this timeout through the call tree.
func (client *Client) SetTimeout(d time.Duration, propagate bool) {
	if !client.active {
		return
	}

	client.default_params.timeout = d
	client.default_params.deadline_propagation = propagate
	client.channel.SetTimeout(d)
}

// Disconnects the channel and disables the client
func (client *Client) Destroy() {
	client.channel.destroy()
	client.channel = RpcChannel{}
	client.active = false
}

// Create a Request to be sent by this client.
// If a previous request has not been finished, this method returns nil.
func (client *Client) NewRequest(service, endpoint string) *Request {
	if client.request_active {
		return nil
	}
	client.request_active = true
	return &Request{client: client, params: client.default_params, service: service, endpoint: endpoint}
}

// Sends a request to the server, asking whether it accepts requests and testing general connectivity.
// Uses a timeout of 1 second.
func (client *Client) IsHealthy() bool {
	return client.IsHealthyWithin(1 * time.Second)
}

// Same as IsHealthy(), but with a configurable timeout
func (client *Client) IsHealthyWithin(d time.Duration) bool {
	rp := client.NewRequest("__CLUSTERRPC", "Health").SetParameters(NewParams().Timeout(d)).Go([]byte{})
	return rp.Ok()
}

// Legacy API -- deprecated!

func (cl *Client) Request(data []byte, service, endpoint string, trace_dest *proto.TraceInfo) ([]byte, error) {
	rp := cl.NewRequest(service, endpoint).SetTrace(trace_dest).Go(data)

	if !rp.Ok() {
		return nil, &rp
	}
	return rp.Payload(), nil
}

func (cl *Client) RequestProtobuf(request, reply pb.Message, service, endpoint string, trace_dest *proto.TraceInfo) error {
	rp := cl.NewRequest(service, endpoint).SetTrace(trace_dest).GoProto(request)

	if !rp.Ok() {
		return &rp
	}
	rp.GetResponseMessage(reply)
	return nil
}
