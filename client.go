package clusterrpc

import (
	"clusterrpc/proto"
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
	channel *zmq.Socket

	logger   *log.Logger
	loglevel LOGLEVEL_T

	name string
	// Slices to allow multiple connections (round-robin)
	raddr           []string
	rport           []uint
	sequence_number uint64
	timeout         time.Duration
	// Used for default calls
	default_service, default_endpoint string
	accept_redirect                   bool
	lock                              sync.Mutex
	eagain_retries                    uint
}

/*
Create a new client that connects to the clusterrpc server at raddr:rport.
The client_name is used for logging purposes. The new client has a default
timeout of 5 seconds (the network operations will time out after this duration
and return an error). Typically, a timed out request will be retried twice
before returning (i.e. the actual timeout is 15 seconds). error is a RequestError object.
The default total timeout 12 seconds. (3 tries * 4 seconds)

*/
func NewClient(client_name, raddr string, rport uint, loglevel LOGLEVEL_T) (cl *Client, e error) {
	return NewClientRR(client_name, []string{raddr}, []uint{rport}, loglevel)
}

/*
Send requests in a round-robin manner to the given servers. Caveat: If one of the peers doesn't respond,
it is still queried, resulting in every len(raddrs)'th request timing out initially (but returning a
response on second try to another peer).

Use this only with stateless services, and only with ones that time out rarely (a reconnect
to one peer as with a Client returned by NewClient() is cheaper than reconnecting to possibly dozens
of servers).

*/
func NewClientRR(client_name string, raddrs []string, rports []uint, loglevel LOGLEVEL_T) (*Client, error) {
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
How often should the client retry after encountering a timeout?
*/
func (cl *Client) SetRetries(n uint) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	cl.eagain_retries = n
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

Returns either a byte slice and nil or an undefined byte slice and a clusterrpc.RequestError.
You can use RequestError's Status() method to get a status string such as STATUS_NOT_FOUND

When not being able to get a response after a timeout (and n reattempts, where n is set using
SetRetries()), we return a RequestError where rqerr.Status() == "STATUS_TIMEOUT".

It can be that, if we're connected to multiple peers and one of the peers crashes, every nth request
(n=number of peers) times out, which makes your application crawl like a snail (although the connection
is automatically re-established after the netsplit ends or the peer comes up again). If you want to prevent
that, you can set the number of retries (SetRetries()) to 0, which makes the client return an error on the first
timeout it encounters (the RequestError.Status() method will return "STATUS_TIMEOUT"). Afterwards, you can
close this client and create a new client to a (possibly) different set of peers and start sending requests.

You could apply this strategy in an environment where you know that your peers crash frequently
(if you know that peers don't crash frequently, but may take too long, you might want to set a number of retries
> 0). Creating a Client is relatively cheap (cheaper than timing out on every second request, that is).

*/
func (cl *Client) Request(data []byte, service, endpoint string) ([]byte, error) {
	return cl.requestInternal(data, service, endpoint, int(cl.eagain_retries))
}

/*
Sends request to the default method (set by SetDefault()). error is actually a RequestError object.
*/
func (cl *Client) RequestDefault(data []byte) ([]byte, error) {
	return cl.Request(data, cl.default_service, cl.default_endpoint)
}

/*
Do one request and clean up afterwards. Not really efficient, but ok for rare use. error is a RequestError
object. If the redirect is invalid, the Status() will return "STATUS_CLIENT_REQUEST_ERROR".

allow_redirect does what it says; it is usually set by a client after following a redirect to
avoid a redirect loop (A redirects to B, B redirects to A)

The pointer to a client can be nil; otherwise, settings such as timeout, logging output and loglevel
are copied from it.
*/
func RequestOneShot(raddr string, rport uint, service, endpoint string, request_data []byte) ([]byte, error) {
	return requestOneShot(raddr, rport, service, endpoint, request_data, true, nil)
}
