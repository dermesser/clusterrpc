package server

import (
	"clusterrpc/proto"
	"errors"
	"log"
	"time"

	pb "github.com/gogo/protobuf/proto"
)

/*
Opaque structure that contains request information
and takes the response.
*/
type Context struct {
	input, result                 []byte
	failed                        bool
	error_message                 string
	redir_host                    string
	redir_port                    uint
	redir_service, redir_endpoint string
	deadline                      time.Time
	// Tracing info
	this_call *proto.TraceInfo

	orig_rq *proto.RPCRequest
	logger  *log.Logger
	// 0 = None, 1 = logged request, 2 = logged response
	log_state int
}

func (srv *Server) newContext(request *proto.RPCRequest, logger *log.Logger) *Context {
	c := new(Context)
	c.input = request.GetData()
	c.failed = false
	c.orig_rq = request
	c.logger = logger

	if request.GetDeadline() > 0 {
		c.deadline = time.Unix(0, 1000*request.GetDeadline())
	}

	if request.GetWantTrace() {
		c.this_call = new(proto.TraceInfo)
		c.this_call.EndpointName = pb.String(request.GetSrvc() + "." + request.GetProcedure())
		c.this_call.MachineName = pb.String(srv.machine_name)
		c.this_call.ReceivedTime = pb.Int64(time.Now().UnixNano() / 1000)
	}

	return c
}

// For half-external use, e.g. by the client package. Returns not nil when the current call tree is traced.
func (c *Context) GetTraceInfo() *proto.TraceInfo {
	return c.this_call
}

// Append traceinfo from child call
func (c *Context) AppendCallTrace(traceinfo *proto.TraceInfo) {
	if traceinfo != nil {
		c.this_call.ChildCalls = append(c.this_call.ChildCalls, traceinfo)
	}
	return
}

/*
Get the data that was sent by the client.
*/
func (c *Context) GetInput() []byte {
	c.rpclogRaw(c.input, log_REQUEST)
	return c.input
}

/*
GetArgument serializes the input in a protocol buffer message.
*/
func (c *Context) GetArgument(msg pb.Message) error {
	err := pb.Unmarshal(c.input, msg)

	if err != nil {
		c.rpclogErr(err)
	} else {
		c.rpclogPB(msg, log_REQUEST)
	}

	return err
}

/*
Get the absolute deadline requested by the caller.
*/
func (c *Context) GetDeadline() time.Time {
	return c.deadline
}

/*
Fail with msg as error message (gets sent back to the client)
*/
func (c *Context) Fail(msg string) {
	c.failed = true
	c.error_message = msg
	c.rpclogErr(errors.New(msg))
}

/*
Set Success flag and the data to return to the caller.
*/
func (c *Context) Success(data []byte) {
	c.result = data
	c.rpclogRaw(data, log_RESPONSE)
}

/*
Set Success flag and the message to return to the caller. Does not do anything special, such as
terminate the calling function etc.
*/
func (c *Context) Return(msg pb.Message) error {
	result, err := pb.Marshal(msg)

	if err != nil {
		return err
	}

	c.result = result

	c.rpclogPB(msg, log_RESPONSE)

	return nil
}

func (cx *Context) toRPCResponse() *proto.RPCResponse {
	rpproto := new(proto.RPCResponse)

	if !cx.failed {
		rpproto.ResponseStatus = proto.RPCResponse_STATUS_OK.Enum()
		rpproto.ResponseData = cx.result
	} else {
		rpproto.ResponseStatus = proto.RPCResponse_STATUS_NOT_OK.Enum()
		rpproto.ErrorMessage = pb.String(cx.error_message)
	}

	// Went over deadline
	if cx.deadline.UnixNano() > 0 && time.Now().UnixNano() > cx.deadline.UnixNano() {
		rpproto.ResponseData = []byte{}
		rpproto.ResponseStatus = proto.RPCResponse_STATUS_MISSED_DEADLINE.Enum()
		rpproto.ErrorMessage = pb.String("Exceeded deadline")
	}

	// Tracing enabled
	if cx.this_call != nil {
		cx.this_call.RepliedTime = pb.Int64(time.Now().UnixNano() / 1000)

		if cx.failed {
			cx.this_call.ErrorMessage = pb.String(cx.error_message)
		}

		rpproto.Traceinfo = cx.this_call
	}

	return rpproto
}
