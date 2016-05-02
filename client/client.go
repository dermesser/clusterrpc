package client

import (
	"clusterrpc/proto"
	golog "log"
	"time"

	pb "github.com/gogo/protobuf/proto"
)

// Various parameters determining how a request is executed. There are builder methods to set the various parameters.
type ClientParams struct {
	accept_redirect      bool
	retries              uint
	deadline_propagation bool
	timeout              time.Duration
}

func NewParams() *ClientParams {
	return &ClientParams{accept_redirect: true, retries: 0, deadline_propagation: false, timeout: 10 * time.Second}
}

// Whether to follow redirects issued by the server. May impact efficiency.
func (p *ClientParams) AcceptRedirects(b bool) *ClientParams {
	p.accept_redirect = b
	return p
}

// How often a request is to be retried. Default: 0
func (p *ClientParams) Retries(r uint) *ClientParams {
	p.retries = r
	return p
}

// Whether to enable deadline propagation; that is, tell the server the time beyond which it doesn't need to bother returning a response.
func (p *ClientParams) DeadlinePropagation(b bool) *ClientParams {
	p.deadline_propagation = b
	return p
}

// Set the timeout; this is used as network timeout and for the deadline propagation, if enabled.
func (p *ClientParams) Timeout(d time.Duration) *ClientParams {
	p.timeout = d
	return p
}

// A (new) client object. It contains a channel
type Client struct {
	channel RpcChannel
	name    string

	active bool
	// To prevent hassle with the REQ state machine, we implement a similar one ourselves; as long as request_active == true, no new requests can be created
	request_active  bool
	sequence_number uint64

	default_params ClientParams

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

// Sends a request to the server, asking whether it accepts requests
func (client *Client) IsHealthy() bool {
	rp := client.NewRequest("__CLUSTERRPC", "Health").SetParameters(NewParams().Timeout(1 * time.Second)).Go([]byte{})
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
	rp.GetResponseMessage(&reply)
	return nil
}
