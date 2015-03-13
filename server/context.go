package server

import (
	"clusterrpc/proto"
	"fmt"
	"time"

	pb "github.com/golang/protobuf/proto"
)

/*
Opaque structure that contains request information
and takes the response.
*/
type Context struct {
	input, result                 []byte
	failed, redirected            bool
	error_message                 string
	redir_host                    string
	redir_port                    uint
	redir_service, redir_endpoint string
	// Tracing info
	this_call *proto.TraceInfo
}

func (srv *Server) newContext(request *proto.RPCRequest) *Context {
	c := new(Context)
	c.input = request.GetData()
	c.failed = false

	if request.GetWantTrace() {
		c.this_call = new(proto.TraceInfo)
		c.this_call.EndpointName = pb.String(request.GetSrvc() + "." + request.GetProcedure())
		c.this_call.MachineName = pb.String(srv.machine_name)
		c.this_call.ReceivedTime = pb.Uint64(uint64(time.Now().UnixNano() / 1000))
	}

	return c
}

// For half-external use, e.g. by the client package
func (c *Context) GetTraceInfo() *proto.TraceInfo {
	return c.this_call
}

// Append call that was made
func (c *Context) AppendCall(traceinfo *proto.TraceInfo) {
	if traceinfo != nil {
		c.this_call.ChildCalls = append(c.this_call.ChildCalls, traceinfo)
	}
	return
}

/*
Get the data that was sent by the client.
*/
func (c *Context) GetInput() []byte {
	return c.input
}

/*
Fail with msg as error message (gets sent back to the client)
*/
func (c *Context) Fail(msg string) {
	c.failed = true
	c.error_message = msg
}

/*
Redirect to the given clusterrpc server. Keep in mind that this is not very efficient
for the client as it has to open a new connection that lives only for the one request.
The client will typically follow only one redirect (i.e. the server we redirected to can
not redirect)
*/
func (c *Context) Redirect(host string, port uint) {
	c.redir_host = host
	c.redir_port = port
	c.redirected = true
}

/*
Tell the client to retry at host:port, service.endpoint. None of the values should be empty,
otherwise the client gets confused.
*/
func (c *Context) RedirectEndpoint(host string, port uint, service, endpoint string) {
	c.Redirect(host, port)
	c.redir_service = service
	c.redir_endpoint = endpoint
}

func (c *Context) Success(data []byte) {
	c.result = data
}

func (cx *Context) toRPCResponse() *proto.RPCResponse {
	rpproto := new(proto.RPCResponse)

	if !cx.failed {
		rpproto.ResponseStatus = proto.RPCResponse_STATUS_OK.Enum()
	} else {
		rpproto.ResponseStatus = proto.RPCResponse_STATUS_NOT_OK.Enum()
		rpproto.ErrorMessage = pb.String(cx.error_message)
	}

	rpproto.ResponseData = cx.result

	if cx.redirected {
		rpproto.RedirHost = pb.String(cx.redir_host)
		rpproto.RedirPort = pb.Uint32(uint32(cx.redir_port))
		rpproto.RedirService = pb.String(cx.redir_service)
		rpproto.RedirEndpoint = pb.String(cx.redir_endpoint)
		rpproto.ResponseStatus = proto.RPCResponse_STATUS_REDIRECT.Enum()

	}
	// Tracing enabled
	if cx.this_call != nil {
		cx.this_call.RepliedTime = pb.Uint64(uint64(time.Now().UnixNano() / 1000))

		if cx.failed {
			cx.this_call.ErrorMessage = pb.String(cx.error_message)
		}

		if cx.redirected {
			cx.this_call.Redirect = pb.String(fmt.Sprintf("%s:%d/%s.%s", cx.redir_host, cx.redir_port,
				cx.redir_service, cx.redir_endpoint))
		}

		rpproto.Traceinfo = cx.this_call
	}

	return rpproto
}
