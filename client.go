package clusterrpc

import (
	"clusterrpc/proto"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	pb "code.google.com/p/goprotobuf/proto"
)

type Callback func([]byte, error)

type Client struct {
	channel         *net.TCPConn
	sequence_number uint64
	logger          *log.Logger
	timeout         time.Duration
	// Used for default calls
	default_service, default_endpoint string
	loglevel                          LOGLEVEL_T
	accept_redirect                   bool
	lock                              sync.Mutex
}

/*
Create a new client that connects to the clusterrpc server at raddr:rport.
The client_name is used for logging purposes. The new client has a default
timeout of 30 seconds (the network operations will time out after this duration
and return an error).

*/
func NewClient(client_name, raddr string, rport int32) (cl *Client, e error) {
	addr := net.TCPAddr{}
	addr.IP = net.ParseIP(raddr)
	addr.Port = int(rport)

	conn, err := net.DialTCP("tcp", nil, &addr)

	if err != nil {
		return nil, err
	}

	cl = new(Client)
	cl.channel = conn
	cl.sequence_number = 0
	cl.logger = log.New(os.Stderr, "clusterrpc.Client "+client_name+": ", log.Lmicroseconds)
	cl.loglevel = LOGLEVEL_ERRORS
	cl.accept_redirect = true
	cl.timeout = 30 * time.Second

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

	cl.timeout = timeout
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
Returns immediately and calls the callback cb once the results are here.
*/
func (cl *Client) RequestAsync(data []byte, service, endpoint string, cb Callback) {

	go func() {
		cb(cl.Request(data, service, endpoint))
	}()

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
	cl.lock.Lock()
	defer cl.lock.Unlock()

	rqproto := proto.RPCRequest{}

	rqproto.SequenceNumber = pb.Uint64(cl.sequence_number)
	cl.sequence_number++

	rqproto.Srvc = pb.String(service)
	rqproto.Procedure = pb.String(endpoint)
	rqproto.Data = pb.String(string(data))

	if cl.timeout > 0 {
		rqproto.Deadline = pb.Uint64(uint64(time.Now().Unix()) + uint64(cl.timeout.Seconds()))
	}

	rq_serialized, pberr := protoToLengthPrefixed(&rqproto)

	if pberr != nil {
		return nil, pberr
	}

	if cl.timeout > 0 {
		cl.channel.SetDeadline(time.Now().Add(cl.timeout))
	}
	n, werr := cl.channel.Write(rq_serialized)

	if cl.loglevel >= LOGLEVEL_DEBUG {
		cl.logger.Println(service+"."+endpoint, rqproto.GetSequenceNumber(), "Sent request")
	}

	if werr != nil {
		if cl.loglevel >= LOGLEVEL_ERRORS {
			cl.logger.Println(service+"."+endpoint, rqproto.GetSequenceNumber(), werr.Error())
		}
		if werr.(net.Error).Timeout() || werr.(net.Error).Temporary() {
			return nil, nil
		}
		return nil, werr
	} else if n < len(rq_serialized) {
		if cl.loglevel >= LOGLEVEL_ERRORS {
			cl.logger.Println(service+"."+endpoint, rqproto.GetSequenceNumber(), "Sent less bytes than provided")
		}
		cl.channel.Close()
		return nil, errors.New("Couldn't send complete messsage")
	}

	if cl.timeout > 0 {
		cl.channel.SetDeadline(time.Now().Add(cl.timeout))
	}
	respprotobytes, rerr := readSizePrefixedMessage(cl.channel)

	if rerr != nil {
		if cl.loglevel >= LOGLEVEL_ERRORS {
			cl.logger.Println(service+"."+endpoint, rqproto.GetSequenceNumber(), "Couldn't read message:", rerr.Error())
		}

		if rerr.(net.Error).Timeout() {
			rerr = RequestError{status: proto.RPCResponse_STATUS_TIMEOUT, message: "Operation timed out"}
		}

		return nil, rerr
	}
	if cl.loglevel >= LOGLEVEL_DEBUG {
		cl.logger.Println(service+"."+endpoint, rqproto.GetSequenceNumber(), "Received response")
	}

	respproto := proto.RPCResponse{}

	err := pb.Unmarshal(respprotobytes, &respproto)

	if err != nil {
		if cl.loglevel >= LOGLEVEL_ERRORS {
			cl.logger.Println(err.Error())
		}
		return nil, err
	}

	if respproto.GetResponseStatus() != proto.RPCResponse_STATUS_OK && respproto.GetResponseStatus() != proto.RPCResponse_STATUS_REDIRECT {
		if cl.loglevel >= LOGLEVEL_WARNINGS {
			cl.logger.Println(service+"."+endpoint, rqproto.GetSequenceNumber(), "Received status other than OK")
		}
		err = RequestError{status: respproto.GetResponseStatus(), message: respproto.GetErrorMessage()}
		return nil, err
	} else if respproto.GetResponseStatus() == proto.RPCResponse_STATUS_REDIRECT {
		if cl.accept_redirect {
			return RequestOneShot(respproto.GetRedirHost(), respproto.GetRedirPort(), service, endpoint, data, false)
		} else {
			return nil, errors.New("Could not follow redirect (redirect loop avoidance)")
		}
	}

	return []byte(respproto.GetResponseData()), nil
}

func (cl *Client) RequestDefault(data []byte) (response []byte, e error) {
	return cl.Request(data, cl.default_service, cl.default_endpoint)
}

/*
Do one request and clean up afterwards.

allow_redirect does what it says; it is usually set by a client after following a redirect to
avoid a redirect loop (A redirects to B, B redirets to A)
*/
func RequestOneShot(raddr string, rport int32, service, endpoint string, request_data []byte, allow_redirect bool) ([]byte, error) {
	cl, err := NewClient("tmp_client", raddr, rport)
	defer cl.Close()

	if err != nil {
		return nil, err
	}

	cl.accept_redirect = allow_redirect

	rsp, err := cl.Request(request_data, service, endpoint)

	if err != nil {
		return rsp, err
	}

	return rsp, nil
}
