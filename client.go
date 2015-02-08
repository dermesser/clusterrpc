package clusterrpc

import (
	"io"
	"log"
	"os"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
)

/*
Synchronous client. This client can only used in a blocking way. It is thread-safe, but
locks and blocks on any function call. It is probably important to know that the default
timeout is 10 seconds, which you might set to another value.
*/
type Client struct {
	channel  *zmq.Socket
	logger   *log.Logger
	loglevel LOGLEVEL_T

	name            string
	raddr           string
	rport           uint
	sequence_number uint64
	timeout         time.Duration
	// Used for default calls
	default_service, default_endpoint string
	accept_redirect                   bool
	lock                              sync.Mutex
	eagain_retries                    int
}

/*
Create a new client that connects to the clusterrpc server at raddr:rport.
The client_name is used for logging purposes. The new client has a default
timeout of 30 seconds (the network operations will time out after this duration
and return an error).

*/
func NewClient(client_name, raddr string, rport uint) (cl *Client, e error) {

	cl = new(Client)
	cl.logger = log.New(os.Stderr, "clusterrpc.Client: ", log.Lmicroseconds)

	cl.sequence_number = 0
	cl.loglevel = LOGLEVEL_ERRORS
	cl.name = client_name
	cl.raddr = raddr
	cl.rport = rport
	cl.accept_redirect = true
	cl.eagain_retries = 3
	cl.timeout = 10 * time.Second

	cl.createChannel()

	return
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
func (cl *Client) SetLoglevel(ll LOGLEVEL_T) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	cl.loglevel = ll
}

/*
Sets the default service and endpoint; those are used for calls to RequestDefault()
*/
func (cl *Client) SetDefault(service, endpoint string) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	cl.default_service = service
	cl.default_endpoint = endpoint
}

/*
Sets the duration in seconds to wait for R/W operations and to use for calculating
the deadline of a Request.

RPCRequests will be sent with the current time plus `timeout` as deadline. If the
server starts processing of the request after the deadline (e.g. because it is loaded and has a rather
long accept() queue, it will respond with a STATUS_TIMEOUT message. The timeout,
however, doesn't mean that the server will stop the handler after the deadline.
It is only used for determining if a response is still wanted.
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

	if cl.loglevel >= LOGLEVEL_INFO {
		cl.logger.Println("Closing client channel")
	}
	cl.channel.Close()
	cl.channel = nil
}

/*
Call a remote procedure service.endpoint with data as input.

Returns either a byte array and nil as error or a status string as the error string.
Returned strings are

		"STATUS_OK" // Not returned in reality
		"STATUS_NOT_FOUND" // Invalid service or endpoint
		"STATUS_NOT_OK" // Handler returned an error.
		"STATUS_SERVER_ERROR" // The clusterrpc server experienced an internal server error
		"STATUS_TIMEOUT" // The server was not able to process the request before the set deadline

Returns nil,nil after a timeout
*/
func (cl *Client) Request(data []byte, service, endpoint string) ([]byte, error) {
	return cl.requestInternal(data, service, endpoint, cl.eagain_retries)
}

func (cl *Client) RequestDefault(data []byte) (response []byte, e error) {
	return cl.Request(data, cl.default_service, cl.default_endpoint)
}

/*
Do one request and clean up afterwards. Not really efficient, but ok for rare use.

allow_redirect does what it says; it is usually set by a client after following a redirect to
avoid a redirect loop (A redirects to B, B redirects to A)

The pointer to a client can be nil; otherwise, settings such as timeout, logging output and loglevel
are copied from it.
*/
func RequestOneShot(raddr string, rport uint, service, endpoint string, request_data []byte) ([]byte, error) {
	return requestOneShot(raddr, rport, service, endpoint, request_data, true, nil)
}
