package client

import (
	"errors"
	"github.com/dermesser/clusterrpc/log"
	"github.com/dermesser/clusterrpc/proto"
	"github.com/dermesser/clusterrpc/server"
	"time"

	pb "github.com/gogo/protobuf/proto"
)

// Various parameters determining how a request is executed. There are builder methods to set the various parameters.
type RequestParams struct {
	accept_redirect      bool
	retries              uint
	deadline_propagation bool
	timeout              time.Duration
}

func NewParams() *RequestParams {
	return &RequestParams{accept_redirect: true, retries: 0, deadline_propagation: false, timeout: 10 * time.Second}
}

// Whether to follow redirects issued by the server. May impact efficiency.
func (p *RequestParams) AcceptRedirects(b bool) *RequestParams {
	p.accept_redirect = b
	return p
}

// How often a request is to be retried. Default: 0
func (p *RequestParams) Retries(r uint) *RequestParams {
	p.retries = r
	return p
}

// Whether to enable deadline propagation; that is, tell the server the time beyond which it doesn't need to bother returning a response.
func (p *RequestParams) DeadlinePropagation(b bool) *RequestParams {
	p.deadline_propagation = b
	return p
}

// Set the timeout; this is used as network timeout and for the deadline propagation, if enabled.
func (p *RequestParams) Timeout(d time.Duration) *RequestParams {
	p.timeout = d
	return p
}

// An RPC request that can be modified before it is sent.
type Request struct {
	client            *Client
	service, endpoint string

	params RequestParams
	ctx    *server.Context
	trace  *proto.TraceInfo

	rpcid         string
	attempt_count int

	// request payload
	payload []byte
}

func (r *Request) SetParameters(p *RequestParams) *Request {
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
	rq.Srvc = &r.service
	rq.WantTrace = pb.Bool(r.trace != nil || (r.ctx != nil && r.ctx.GetTraceInfo() != nil))
	rq.RpcId = &r.rpcid
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
	r.rpcid = log.GetLogToken()
	r.payload = payload

	before := time.Now()
	timer := time.NewTimer(r.params.timeout)
	if r.params.timeout > r.client.defaultParams.timeout {
		r.client.channel.SetTimeout(r.params.timeout)
	}
	defer timer.Stop()
	select {
	case <-r.client.request_active:
		r.params.timeout = r.params.timeout - time.Now().Sub(before)
		rp := r.callNextFilter(0)
		r.client.request_active <- true
		return rp
	case <-timer.C:
		return Response{err: errors.New("deadline expired on client")}
	}
}
