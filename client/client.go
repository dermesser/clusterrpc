package client

import (
	"bytes"
	"clusterrpc/log"
	"clusterrpc/proto"
	smgr "clusterrpc/securitymanager"
	"clusterrpc/server"
	"errors"
	"fmt"
	golog "log"
	"strings"
	"sync"
	"time"

	pb "github.com/gogo/protobuf/proto"
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

	name string

	// Slices to allow multiple connections (round-robin)
	raddr []string
	rport []uint

	sequence_number uint64

	timeout   time.Duration
	last_used time.Time

	// Used for default calls
	accept_redirect      bool
	eagain_retries       uint
	deadline_propagation bool
	do_healthcheck       bool

	heartbeat_active bool

	security_manager *smgr.ClientSecurityManager
	rpclogger        *golog.Logger
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
func NewClient(client_name, raddr string, rport uint,
	security_manager *smgr.ClientSecurityManager) (cl *Client, e error) {
	return NewClientRR(client_name, []string{raddr}, []uint{rport}, security_manager)
}

/*
Create client that sends requests in a round-robin manner to the given servers. If one of the servers
doesn't respond, it is taken out of the set of servers being queried. It is recommended to re-connect
the client regularly to prevent overloading one server (because servers don't necessarily re-join the pool)

Use this only with fast (so you can set a very low timeout), stateless services (because of round-robin),
and only with ones that time out (or fail) rarely (a reconnect to one peer as with a Client
returned by NewClient() is cheaper than reconnecting to possibly dozens of servers). A typical
use-case might be a lookup or cache server.
use-case might be a lookup or cache server.

*/
func NewClientRR(client_name string, raddrs []string, rports []uint, security_manager *smgr.ClientSecurityManager) (*Client, error) {
	if len(raddrs) != len(rports) {
		return nil, &RequestError{status: proto.RPCResponse_STATUS_CLIENT_CALLED_WRONG, err: errors.New("Mismatch between raddrs/rports lengths")}
	}
	cl := new(Client)

	cl.sequence_number = 0
	cl.name = client_name
	cl.raddr = raddrs
	cl.rport = rports
	cl.accept_redirect = true
	cl.eagain_retries = 0
	cl.timeout = 4 * time.Second
	cl.deadline_propagation = false
	cl.security_manager = security_manager

	err := cl.createChannel()

	if err != nil {
		return nil, err
	}

	return cl, err
}

/*
How often should the client retry after encountering a SEND (!) timeout?

If a RECEIVE operation times out, clusterrpc will not retry the call!
*/
func (cl *Client) SetRetries(n uint) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	cl.eagain_retries = n
}

/*
Log all RPCs made by this client to this logging device; either as hex/raw strings or protobuf strings.
*/
func (cl *Client) SetRPCLogger(l *golog.Logger) {
	cl.rpclogger = l
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
When health checking is enabled, every request will only be made after getting a successful
response from the server's health checking endpoint. This will of course result in decreased
performance, but helps increasing reliability.
*/
func (cl *Client) SetHealthcheck(enabled bool) {
	cl.do_healthcheck = enabled
}

/*
Disable the client. Following calls will result in nil dereference.
TODO: Maybe do not do the above stated.
*/
func (cl *Client) Close() {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	if cl.channel == nil {
		return
	}

	log.CRPC_log(log.LOGLEVEL_INFO, "Closing client channel", cl.name)
	cl.channel.SetLinger(0)
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
Send a request to the backend and check if it responds in time, i.e. whether it's healthy or not.
*/
func (cl *Client) IsHealthy() bool {
	return cl.doHealthCheck()
}

func (cl *Client) RunHeartbeat(ival time.Duration) {
	if !cl.heartbeat_active {

		log.CRPC_log(log.LOGLEVEL_INFO, "Running ping sender for client", cl.name, cl.heartbeat_active, "ival", ival)

		cl.heartbeat_active = true

		go func() {

			for cl.heartbeat_active && cl.channel != nil {
				time.Sleep(ival)
				cl.doHeartBeat()
			}
		}()
	}

}

func (cl *Client) StopHeartbeat() {
	cl.heartbeat_active = false
}

func (cl *Client) formatRemoteHosts() string {
	if len(cl.raddr) == 1 {
		return cl.raddr[0]
	} else {
		str := ""
		for _, h := range cl.raddr {
			str += h + ","
		}
		return str
	}
}

/*
Call a remote procedure service.endpoint with data as input.

Returns either a byte slice and nil or an undefined byte slice and a *clusterrpc.RequestError
(wrapped in an error interface value, of course). You can use RequestError's Status() method
to get a status string such as STATUS_NOT_FOUND.

When not being able to get a response after a timeout, we return a *RequestError where
rqerr.Status() == "STATUS_TIMEOUT". This is probably due to a completely overloaded server,
a crashed server or a netsplit. There is no automatic re-sending requests if receiving has failed,
only if sending was not successful. This decreases the probability of two RPCs just because one server
was slow.

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
points to. Tracing calls is however impacting performance negatively, so only a fraction of calls should
be traced. The TraceInfo structure is essentially a tree of the calls made on behalf of this
original call.
*/
func (cl *Client) Request(data []byte, service, endpoint string, trace_dest *proto.TraceInfo) ([]byte, error) {

	cl.rpclogRaw(service, endpoint, data, log_REQUEST)

	if cl.do_healthcheck {
		result := cl.doHealthCheck()
		if !result {
			return nil, &RequestError{status: proto.RPCResponse_STATUS_UNHEALTHY, err: errors.New("RPC backend unhealthy")}
		}
	}

	b, err := cl.request(nil, trace_dest, data, service, endpoint)

	if err == nil {
		cl.rpclogRaw(service, endpoint, b, log_RESPONSE)
	} else {
		cl.rpclogErr(service, endpoint, err)
	}

	return b, err
}

/*
Call another service from within a handler.

This takes a context which is used for deadline propagation and full-stack call tracing.
It is recommended to use this in handlers, as plain Request() will not carry important
call information and make traces and deadline propagation completely unavailable.
*/
func (cl *Client) RequestWithCtx(cx *server.Context, data []byte, service, endpoint string) ([]byte, error) {

	cl.rpclogRaw(service, endpoint, data, log_REQUEST)

	if cl.do_healthcheck {
		result := cl.doHealthCheck()
		if !result {
			return nil, &RequestError{status: proto.RPCResponse_STATUS_UNHEALTHY, err: errors.New("RPC backend unhealthy")}
		}
	}

	b, err := cl.request(cx, nil, data, service, endpoint)

	if err == nil {
		cl.rpclogRaw(service, endpoint, b, log_RESPONSE)
	} else {
		cl.rpclogErr(service, endpoint, err)
	}

	return b, err
}

/*
Use protobuf message objects instead of raw byte slices.

request is the request protocol buffer which is to be sent, reply (an output argument) will contain the
message the server sent as reply. Usually, pb.Message is implemented by pointer types, so this works
without explicitly using pointer arguments.

reply will only contain the response if the returned error is nil.

For the other arguments, refer to the comments on plain Request()
*/
func (cl *Client) RequestProtobuf(request, reply pb.Message, service, endpoint string, trace_dest *proto.TraceInfo) error {

	cl.rpclogPB(service, endpoint, request, log_REQUEST)

	if cl.do_healthcheck {
		result := cl.doHealthCheck()
		if !result {
			return &RequestError{status: proto.RPCResponse_STATUS_UNHEALTHY, err: errors.New("RPC backend unhealthy")}
		}
	}

	serialized_request, err := pb.Marshal(request)

	if err != nil {
		cl.rpclogErr(service, endpoint, err)
		return err
	}

	response_bytes, err := cl.request(nil, trace_dest, serialized_request, service, endpoint)

	if err != nil {
		cl.rpclogErr(service, endpoint, err)
		return err
	}

	err = pb.Unmarshal(response_bytes, reply)

	if err != nil {
		cl.rpclogErr(service, endpoint, err)
		return err
	}

	cl.rpclogPB(service, endpoint, reply, log_RESPONSE)

	return nil
}

func (cl *Client) RequestProtobufWithCtx(cx *server.Context, request, reply pb.Message, service, endpoint string) error {

	cl.rpclogPB(service, endpoint, request, log_REQUEST)

	if cl.do_healthcheck {
		result := cl.doHealthCheck()
		if !result {
			return &RequestError{status: proto.RPCResponse_STATUS_UNHEALTHY, err: errors.New("RPC backend unhealthy")}
		}
	}

	serialized_request, err := pb.Marshal(request)

	if err != nil {
		cl.rpclogErr(service, endpoint, err)
		return err
	}

	response_bytes, err := cl.request(cx, nil, serialized_request, service, endpoint)

	if err != nil {
		cl.rpclogErr(service, endpoint, err)
		return err
	}

	err = pb.Unmarshal(response_bytes, reply)

	if err != nil {
		cl.rpclogErr(service, endpoint, err)
		return err
	}

	cl.rpclogPB(service, endpoint, reply, log_RESPONSE)

	return nil
}
