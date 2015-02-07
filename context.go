package clusterrpc

type Context struct {
	input, result      []byte
	failed, redirected bool
	errorMessage       string
	redir_host         string
	redir_port         uint32
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

func (c *Context) Redirect(host string, port uint32) {
	c.redir_host = host
	c.redir_port = port
	c.redirected = true
}

func (c *Context) Success(data []byte) {
	c.result = data
}
