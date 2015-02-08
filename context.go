package clusterrpc

import (
	"clusterrpc/proto"

	pb "code.google.com/p/goprotobuf/proto"
)

type Context struct {
	input, result      []byte
	failed, redirected bool
	errorMessage       string
	redir_host         string
	redir_port         uint
}

func NewContext(input []byte) *Context {
	c := new(Context)
	c.input = input
	c.failed = false
	return c
}

func (c *Context) GetInput() []byte {
	return c.input
}

func (c *Context) Fail(msg string) {
	c.failed = true
	c.errorMessage = msg
}

func (c *Context) Redirect(host string, port uint) {
	c.redir_host = host
	c.redir_port = port
	c.redirected = true
}

func (c *Context) Success(data []byte) {
	c.result = data
}

func (cx *Context) toRPCResponse() proto.RPCResponse {
	rpproto := proto.RPCResponse{}
	rpproto.ResponseStatus = new(proto.RPCResponse_Status)

	if !cx.failed {
		*rpproto.ResponseStatus = proto.RPCResponse_STATUS_OK
	} else {
		*rpproto.ResponseStatus = proto.RPCResponse_STATUS_NOT_OK
		rpproto.ErrorMessage = pb.String(cx.errorMessage)
	}

	rpproto.ResponseData = pb.String(string(cx.result))

	if cx.redirected {
		rpproto.RedirHost = pb.String(cx.redir_host)
		rpproto.RedirPort = pb.Uint32(uint32(cx.redir_port))
		*rpproto.ResponseStatus = proto.RPCResponse_STATUS_REDIRECT
	}

	return rpproto
}
