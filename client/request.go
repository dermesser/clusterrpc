package client

import (
	"clusterrpc/log"
	"clusterrpc/proto"
	"clusterrpc/server"
	"time"

	pb "github.com/gogo/protobuf/proto"
)

// An RPC request that can be modified before it is sent.
type Request struct {
	client            *Client
	service, endpoint string

	params ClientParams
	ctx    *server.Context
	trace  *proto.TraceInfo

	rpcid         string
	attempt_count int

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
	rq.Srvc = &r.service
	rq.WantTrace = pb.Bool(r.trace != nil || r.ctx != nil)
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
	rp := r.callNextFilter(0)
	r.client.request_active = false
	return rp
}
