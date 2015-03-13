package server

import (
	"clusterrpc/proto"
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
	errorMessage                  string
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
		c.this_call.ServiceName = pb.String(request.GetSrvc() + "." + request.GetProcedure())
		c.this_call.MachineName = pb.String(srv.machine_name)
		c.this_call.ReceivedTime = pb.Uint64(uint64(time.Now().UnixNano() / 1000))
	}

	return c
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
	c.errorMessage = msg
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
		rpproto.ErrorMessage = pb.String(cx.errorMessage)
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
		rpproto.Traceinfo = cx.this_call
	}

	return rpproto
}
