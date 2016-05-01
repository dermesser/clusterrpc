package client

import (
	"bytes"
	"clusterrpc/proto"
	"clusterrpc/server"
	"fmt"
	golog "log"
	"strings"
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

type Response struct {
	err      error
	response *proto.RPCResponse
}

// Check whether the request was successful
func (rp *Response) Ok() bool {
	return rp.err == nil && rp.response.GetResponseStatus() == proto.RPCResponse_STATUS_OK
}

// Returns the response payload
func (rp *Response) Payload() []byte {
	return rp.response.GetResponseData()
}

// Unmarshals the response into msg
func (rp *Response) GetResponseMessage(msg *pb.Message) error {
	return pb.Unmarshal(rp.response.GetResponseData(), *msg)
}

// Get the error that has occurred.
func (rp *Response) Error() string {
	if rp.err != nil {
		return rp.err.Error()
	} else if rp.response.GetResponseStatus() != proto.RPCResponse_STATUS_OK {
		return rp.response.GetResponseStatus().String()
	} else {
		return ""
	}
}

// An RPC request that can be modified before it is sent.
type Request struct {
	client            *Client
	service, endpoint string
	sequence_number   uint64

	params ClientParams
	ctx    *server.Context
	trace  *proto.TraceInfo

	// request payload
	payload []byte
}

func (r *Request) SetParameters(p *ClientParams) *Request {
	r.params = *p
	return r
}
func (r *Request) SetContext(c *server.Context) *Request {
	r.ctx = c
	return r
}
func (r *Request) SetTrace(t *proto.TraceInfo) *Request {
	r.trace = t
	return r
}

func (r *Request) callNextFilter(index int) Response {
	if len(r.client.filters) < index+1 {
		panic("Bad filter setup: Not enough filters.")
	}
	return r.client.filters[index](r, index+1)
}

func (r *Request) makeRPCRequestProto() *proto.RPCRequest {
	rq := new(proto.RPCRequest)
	rq.CallerId = &r.client.name
	rq.Data = r.payload
	rq.Procedure = &r.endpoint
	rq.SequenceNumber = &r.sequence_number
	rq.Srvc = &r.service
	rq.WantTrace = pb.Bool(r.trace != nil || r.ctx != nil)
	if r.params.deadline_propagation {
		rq.Deadline = pb.Int64((time.Now().UnixNano() + r.params.timeout.Nanoseconds()) / 1000)
	}
	return rq
}

// Send a request with a serialized protocol buffer
func (r *Request) GoProto(msg pb.Message) Response {
	payload, err := pb.Marshal(msg)
	if err != nil {
		return Response{err: err}
	}
	return r.Go(payload)
}

// Send a request.
func (r *Request) Go(payload []byte) Response {
	r.payload = payload
	rp := r.callNextFilter(0)
	r.client.request_active = false
	return rp
}

// A ClientFilter is a function that is called with a request and fulfills a certain task.
// Filters are stacked in Client.filters; filters[0] is called first, and calls in turn filters[1]
// until the last filter sends the message off to the network.
type ClientFilter (func(rq *Request, next_filter int) Response)

// TODO: Add RedirectFilter
var default_filters = []ClientFilter{TraceMergeFilter, TimeoutFilter, RetryFilter, SendFilter}

// Appends the received trace info to context or requested trace.
func TraceMergeFilter(rq *Request, next int) Response {
	response := rq.callNextFilter(next)

	if response.response != nil {
		if rq.trace != nil {
			*rq.trace = *response.response.GetTraceinfo()
		}
		if rq.ctx != nil {
			rq.ctx.AppendCallTrace(response.response.GetTraceinfo())
		}
	}
	return response
}

// Sets appropriate timeouts on the socket, only for this request
func TimeoutFilter(rq *Request, next int) Response {
	old_timeout, err := rq.client.channel.channel.GetRcvtimeo()

	if err == nil {
		if rq.ctx != nil && !rq.ctx.GetDeadline().IsZero() {
			rq.client.channel.SetTimeout(rq.ctx.GetDeadline().Sub(time.Now()))
		} else {
			rq.client.channel.SetTimeout(rq.params.timeout)
		}
		defer rq.client.channel.SetTimeout(old_timeout)
	}

	return rq.callNextFilter(next)
}

// Implements redirects: A server can tell us to follow a redirect. This is
// expensive in general because it involves setting up and tearing down a
// completely new client. It also doesn't work well with security-enabled
// RPCs.
func RedirectFilter(rq *Request, next int) Response {
	// NOTE: This filter is unimplemented, because it is being phased out. Redirections are an unnecessary feature.
	return rq.callNextFilter(next)
}

// A filter that retries a request according to the request's parameters.
func RetryFilter(rq *Request, next int) Response {
	attempts := int(rq.params.retries + 1)

	last_response := Response{}
	for i := 0; i < attempts; i++ {
		response := rq.callNextFilter(next)

		if response.err == nil {
			return response
		}
		last_response = response
		rq.client.channel.Reconnect()
	}
	return Response{err: fmt.Errorf("Retried %d times without success: %s", rq.params.retries, last_response.err.Error())}
}

// Send a request and wait for it to complete. Must be the last filter in the stack
func SendFilter(rq *Request, next int) Response {
	// Enforce that this is the last filter.
	if len(rq.client.filters) != next {
		panic("Bad filter setup")
	}

	message := rq.makeRPCRequestProto()
	payload, err := message.Marshal()

	if err != nil {
		panic("Could not serialize RPCRequest!!")
	}

	rq.client.last_sent = time.Now()
	err = rq.client.channel.sendMessage(payload)

	if err != nil {
		return Response{err: err}
	}

	response_payload, err := rq.client.channel.receiveMessage()

	if err != nil {
		return Response{err: err}
	}

	response := new(proto.RPCResponse)
	err = response.Unmarshal(response_payload)

	if err != nil {
		return Response{err: err}
	}

	return Response{response: response}
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

// Utility functions

const TRACE_INFO_TIME_FORMAT = "Mon Jan _2 15:04:05.999 2006"

/*
Formats the TraceInfo data structure.
*/
func FormatTraceInfo(ti *proto.TraceInfo, indent int) string {
	if ti == nil {
		return ""
	}
	indent_string := strings.Repeat(" ", indent)
	buf := bytes.NewBuffer(nil)

	fmt.Fprintf(buf, "%sReceived: %s\n", indent_string,
		time.Unix(0, int64(1000*ti.GetReceivedTime())).UTC().Format(TRACE_INFO_TIME_FORMAT))

	fmt.Fprintf(buf, "%sReplied: %s\n", indent_string,
		time.Unix(0, int64(1000*ti.GetRepliedTime())).UTC().Format(TRACE_INFO_TIME_FORMAT))

	if ti.GetMachineName() != "" {
		fmt.Fprintf(buf, "%sMachine: %s\n", indent_string, ti.GetMachineName())
	}
	if ti.GetEndpointName() != "" {
		fmt.Fprintf(buf, "%sEndpoint: %s\n", indent_string, ti.GetEndpointName())
	}
	if ti.GetErrorMessage() != "" {
		fmt.Fprintf(buf, "%sError: %s\n", indent_string, ti.GetErrorMessage())
	}
	if ti.GetRedirect() != "" {
		fmt.Fprintf(buf, "%sRedirect: %s\n", indent_string, ti.GetRedirect())
	}

	if len(ti.GetChildCalls()) > 0 {
		for _, call_traceinfo := range ti.GetChildCalls() {
			fmt.Fprintf(buf, FormatTraceInfo(call_traceinfo, indent+2))
			fmt.Fprintf(buf, "\n")
		}
	}

	return buf.String()
}
