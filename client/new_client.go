package client

import (
	golog "log"
	"time"
)

// An RPC request that can be modified before it is sent.
type Request struct {
	service, endpoint string
	sequence_number   uint64

	finished_callback func()

	params ClientParams
}

func (r *Request) SetParameters(p *ClientParams) {
	r.params = *p
}

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
func (p *ClientParams) AcceptRedirects(b bool) *ClientParams {
	p.accept_redirect = b
	return p
}
func (p *ClientParams) Retries(r uint) *ClientParams {
	p.retries = r
	return p
}
func (p *ClientParams) DeadlinePropagation(b bool) *ClientParams {
	p.deadline_propagation = b
	return p
}
func (p *ClientParams) Timeout(d time.Duration) *ClientParams {
	p.timeout = d
	return p
}

// A (new) client object. It contains a channel
type _Client struct {
	channel RpcChannel
	name    string

	active bool
	// To prevent hassle with the REQ state machine, we implement a similar one ourselves; as long as request_active == true, no new requests can be created
	request_active  bool
	sequence_number uint64

	default_params ClientParams

	last_sent time.Time
	rpclogger *golog.Logger
}

// Creates a new client from the channel.
// Don't share a channel among two concurrently active clients.
func _NewClient(name string, channel RpcChannel) _Client {
	return _Client{name: name, channel: channel, active: true}
}

// Set socket timeout (default 10s) and whether to propagate this timeout through the call tree.
func (client *_Client) SetTimeout(d time.Duration, propagate bool) {
	if !client.active {
		return
	}

	client.default_params.timeout = d
	client.default_params.deadline_propagation = propagate
	client.channel.SetTimeout(d)
}

// Disconnects the channel and disables the client
func (client *_Client) Destroy() {
	client.channel.destroy()
	client.channel = RpcChannel{}
	client.active = false
}

// Create a Request to be sent by this client.
// If a previous request has not been finished, this method returns nil.
func (client *_Client) Request(service, endpoint string) *Request {
	if client.request_active {
		return nil
	}
	client.request_active = true

	// called by the Request.Send() method after having received an answer.
	callback := func() { client.request_active = false }

	return &Request{params: client.default_params, service: service, endpoint: endpoint, finished_callback: callback}
}
