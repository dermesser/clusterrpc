package client

import (
	"bytes"
	"clusterrpc"
	"clusterrpc/proto"
	"clusterrpc/server"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	pb "github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
)

/*
Synchronous client. This client can only used in a blocking way. It is thread-safe, but
locks and blocks on any function call. It is probably important to know that the default
timeout is 10 seconds, which you might set to another value.
*/
type Client struct {
	lock    sync.Mutex
	channel *zmq.Socket

	logger   *log.Logger
	loglevel clusterrpc.LOGLEVEL_T

	name string

	// Slices to allow multiple connections (round-robin)
	raddr           []string
	rport           []uint
	sequence_number uint64
	timeout         time.Duration

	// Used for default calls
	accept_redirect      bool
	eagain_retries       uint
	deadline_propagation bool
}

/*
Create a new client that connects to the clusterrpc server at raddr:rport.
The client_name is used for logging purposes. The new client has a default
timeout of 4 seconds (the network operations will time out after this duration
and return an error). Typically, a timed out request will be retried twice
before returning (i.e. the actual timeout is 12 seconds). error is a RequestError object with err.Status()
being "STATUS_TIMEOUT" in case of a timeout (consolt Status()'s documentation for a list
of return codes)

*/
func NewClient(client_name, raddr string, rport uint, loglevel clusterrpc.LOGLEVEL_T) (cl *Client, e error) {
	return NewClientRR(client_name, []string{raddr}, []uint{rport}, loglevel)
}

/*
Send requests in a round-robin manner to the given servers. If one of the servers doesn't respond,
it is taken out of the set of servers being queried. It is recommended to re-connect the client
regularly to prevent overloading one server (because servers don't necessarily re-join the pool)

Use this only with fast (so you can set a very low timeout), stateless services (because of round-robin),
and only with ones that time out (or fail) rarely (a reconnect to one peer as with a Client
returned by NewClient() is cheaper than reconnecting to possibly dozens of servers). A typical
use-case might be a lookup or cache server.

*/
func NewClientRR(client_name string, raddrs []string, rports []uint, loglevel clusterrpc.LOGLEVEL_T) (*Client, error) {
	if len(raddrs) != len(rports) {
		return nil, RequestError{status: proto.RPCResponse_STATUS_CLIENT_CALLED_WRONG, message: "raddrs and rports differ in length"}
	}
	cl := new(Client)
	cl.logger = log.New(os.Stderr, "clusterrpc.Client: ", log.Lmicroseconds)

	cl.sequence_number = 0
	cl.loglevel = loglevel
	cl.name = client_name
	cl.raddr = raddrs
	cl.rport = rports
	cl.accept_redirect = true
	cl.eagain_retries = 2
	cl.timeout = 4 * time.Second // makes 12 seconds as total timeout
	cl.deadline_propagation = true

	err := cl.createChannel()

	if err != nil {
		return nil, err
	}

	return cl, err
}

/*
Change the writer to which the client logs operations.
*/
func (cl *Client) SetLoggingOutput(w io.Writer) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	cl.logger = log.New(w, cl.logger.Prefix(), cl.logger.Flags())
}

/*
Set the logger of the client to a custom one.
*/
func (cl *Client) SetLogger(l *log.Logger) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	cl.logger = l
}

/*
Define which errors/situations to log
*/
func (cl *Client) SetLoglevel(ll clusterrpc.LOGLEVEL_T) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	cl.loglevel = ll
}

/*
How often should the client retry after encountering a timeout?
*/
func (cl *Client) SetRetries(n uint) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	cl.eagain_retries = n
}

/*
Enable/Disable deadline propagation (default: enabled)
*/
func (cl *Client) SetDeadlinePropagation(on bool) {
	cl.deadline_propagation = on
}

/*
Sets the duration to wait for R/W operations and to use for calculating the deadline of a Request.
The deadline is propagated to following calls the called endpoint may issue. If the deadline has
already been exceeded, servers will not process the request but throw it away.

RPC requests will be sent with the current time plus `timeout` as deadline. If the
server starts processing of the request after the deadline (e.g. because it is loaded and has a rather
long accept() queue, it will respond with a STATUS_TIMEOUT message.
*/
func (cl *Client) SetTimeout(timeout time.Duration) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	if timeout == 0 {
		timeout = -1
	}
	cl.timeout = timeout
	cl.channel.SetSndtimeo(timeout)
	cl.channel.SetRcvtimeo(timeout)
}

/*
Disable the client. Following calls will result in nil dereference.
TODO: Maybe do not do the above stated.
*/
func (cl *Client) Close() {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	if cl.loglevel >= clusterrpc.LOGLEVEL_INFO {
		cl.logger.Println("Closing client channel")
	}
	cl.channel.Close()
	cl.channel = nil
}

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

/*
Call a remote procedure service.endpoint with data as input.

Returns either a byte slice and nil or an undefined byte slice and a clusterrpc.RequestError
(wrapped in an error interface value, of course). You can use RequestError's Status() method
to get a status string such as STATUS_NOT_FOUND.

When not being able to get a response after a timeout (and n reattempts, where n has been set using
SetRetries()), we return a RequestError where rqerr.Status() == "STATUS_TIMEOUT". This is probably
due to a completely overloaded server, a crashed server or a netsplit.

There's something to pay attention to when dealing with multiple servers (NewClientRR()); if one server
becomes unresponsive (i.e. the attempt to receive a response timed out), the client makes another attempt
at the next server of the set. The unresponsive server is taken out of the set; in theory, the client
should try to reconnect to the unresponsive server (this was unfortunately not yet observed in practice).

With this setup, once you start getting STATUS_TIMEOUT errors from Request(), you know that none
of the servers you have specified is responding anymore, and you probably want to reconnect
to another set of servers. Another possible caveat is that a single hot client can put too much
load on a server if the client's request load is so high that it only can be managed using the rotation
between multiple servers. If possible, avoid such clients.

If trace_dest is not nil, a TraceInfo protobuf struct will be placed at the location the pointer
points to. Tracing calls is however impacting performance slightly, so only a fraction of calls should
be traced. The TraceInfo structure is essentially a tree of the calls made on behalf of this
original call.
*/
func (cl *Client) Request(data []byte, service, endpoint string, trace_dest *proto.TraceInfo) ([]byte, error) {
	return cl.request(nil, trace_dest, data, service, endpoint, int(cl.eagain_retries))
}

/*
Call another service from within a handler.

This takes a context which is used for deadline propagation and full-stack call tracing.
It is recommended to use this in handlers, as plain Request() will not carry important
call information and make traces and deadline propagation completely unavailable.
*/
func (cl *Client) RequestWithCtx(cx *server.Context, data []byte, service, endpoint string) ([]byte, error) {
	return cl.request(cx, nil, data, service, endpoint, int(cl.eagain_retries))
}

/*
Does the same as RequestWithCtx, but also stores the trace in the specified destination. This is useful
in case a service wants to receive traces of the calls it issues.
*/
func (cl *Client) RequestWithCtxAndTrace(cx *server.Context, trace_dest *proto.TraceInfo, data []byte,
	service, endpoint string) ([]byte, error) {
	return cl.request(cx, trace_dest, data, service, endpoint, int(cl.eagain_retries))
}

/*
Use protobuf message objects instead of raw byte slices.

request is the request protocol buffer which is to be sent, reply (an output argument) will contain the
message the server sent as reply. Usually, pb.Message is implemented by pointer types, so this works
without explicitly using pointer arguments.
*/
func (cl *Client) RequestProtobuf(request, reply pb.Message, service, endpoint string, trace_dest *proto.TraceInfo) error {
	serialized_request, err := pb.Marshal(request)

	if err != nil {
		return err
	}

	response_bytes, err := cl.request(nil, trace_dest, serialized_request, service, endpoint, int(cl.eagain_retries))

	if err != nil {
		return err
	}

	err = pb.Unmarshal(response_bytes, reply)

	if err != nil {
		return err
	}

	return nil
}
