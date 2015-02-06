package clusterrpc

import (
	"clusterrpc/proto"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	pb "code.google.com/p/goprotobuf/proto"
	zmq4 "github.com/pebbe/zmq4"
)

/*
Synchronous client. This client can only used in a blocking way. It is thread-safe, but
blocks on any function call (therefore don't use it if you expect contention)
*/
type Client struct {
	channel  *zmq4.Socket
	logger   *log.Logger
	loglevel LOGLEVEL_T

	name            string
	sequence_number uint64
	timeout         time.Duration
	// Used for default calls
	default_service, default_endpoint string
	accept_redirect                   bool
	lock                              sync.Mutex
}

/*
Create a new client that connects to the clusterrpc server at raddr:rport.
The client_name is used for logging purposes. The new client has a default
timeout of 30 seconds (the network operations will time out after this duration
and return an error).

*/
func NewClient(client_name, raddr string, rport uint32) (cl *Client, e error) {

	cl = new(Client)
	cl.logger = log.New(os.Stderr, "clusterrpc.Client: ", log.Lmicroseconds)

	var err error
	cl.channel, err = zmq4.NewSocket(zmq4.REQ)

	if err != nil {
		cl.logger.Println("Error when creating Req socket:", err.Error())
		return nil, err
	}

	err = cl.channel.Connect(fmt.Sprintf("tcp://%s:%d", raddr, rport))

	if err != nil {
		cl.logger.Println("Error when connecting Req socket:", err.Error())
		return nil, err
	}

	cl.sequence_number = 0
	cl.loglevel = LOGLEVEL_ERRORS
	cl.name = client_name
	cl.accept_redirect = true

	cl.channel.SetSndtimeo(30 * time.Second)
	cl.channel.SetRcvtimeo(30 * time.Second)

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
		cl.timeout = timeout
		cl.channel.SetSndtimeo(timeout)
		cl.channel.SetRcvtimeo(timeout)
	}
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
	cl.lock.Lock()
	defer cl.lock.Unlock()

	rqproto := proto.RPCRequest{}

	rqproto.SequenceNumber = pb.Uint64(cl.sequence_number)
	cl.sequence_number++

	rqproto.Srvc = pb.String(service)
	rqproto.Procedure = pb.String(endpoint)
	rqproto.Data = pb.String(string(data))
	rqproto.CallerId = pb.String(cl.name)

	rq_serialized, pberr := pb.Marshal(&rqproto)

	if pberr != nil {
		if cl.loglevel >= LOGLEVEL_WARNINGS {
			cl.logger.Println("PB error!", pberr.Error())
		}
		return nil, pberr
	}

	_, err := cl.channel.Send(string(rq_serialized), 0)

	if err != nil {
		if cl.loglevel >= LOGLEVEL_ERRORS {
			cl.logger.Printf("[%s/%d] Could not send message to %s. Error: %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint, err.Error())
		}
		return nil, err
	} else {
		if cl.loglevel >= LOGLEVEL_DEBUG {
			cl.logger.Printf("[%s/%d] Sent request to %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint)
		}
	}

	msg, err := cl.channel.Recv(0)

	if err != nil {
		if cl.loglevel >= LOGLEVEL_ERRORS {
			cl.logger.Printf("[%s/%d] Could not receive response from %s, error %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint, err.Error())
		}

		return nil, err
	}
	if cl.loglevel >= LOGLEVEL_DEBUG {
		cl.logger.Printf("[%s/%d] Received response from %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint)
	}

	respproto := proto.RPCResponse{}

	err = pb.Unmarshal([]byte(msg), &respproto)

	if err != nil {
		if cl.loglevel >= LOGLEVEL_ERRORS {
			cl.logger.Printf("[%s/%d] Error when unmarshaling response: %s\n", cl.name, rqproto.GetSequenceNumber(), err.Error())
		}
		return nil, err
	}

	if respproto.GetResponseStatus() != proto.RPCResponse_STATUS_OK && respproto.GetResponseStatus() != proto.RPCResponse_STATUS_REDIRECT {
		if cl.loglevel >= LOGLEVEL_WARNINGS {
			cl.logger.Printf("[%s/%d] Received status other than ok from %s: %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint, StatusToString(respproto.GetResponseStatus()))
		}
		err = RequestError{status: respproto.GetResponseStatus(), message: respproto.GetErrorMessage()}
		return nil, err
	} else if respproto.GetResponseStatus() == proto.RPCResponse_STATUS_REDIRECT {
		if cl.accept_redirect {
			return RequestOneShot(respproto.GetRedirHost(), respproto.GetRedirPort(), service, endpoint, data, false, cl)
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
Do one request and clean up afterwards. Not really efficient, but ok for rare use.

allow_redirect does what it says; it is usually set by a client after following a redirect to
avoid a redirect loop (A redirects to B, B redirects to A)

The pointer to a client can be nil; otherwise, settings such as timeout, logging output and loglevel
are copied from it.
*/
func RequestOneShot(raddr string, rport uint32, service, endpoint string, request_data []byte, allow_redirect bool, settings_cl *Client) ([]byte, error) {
	cl, err := NewClient("tmp_client", raddr, rport)
	defer cl.Close()

	if err != nil {
		return nil, err
	}

	cl.accept_redirect = allow_redirect

	if settings_cl != nil {
		cl.loglevel = settings_cl.loglevel
		cl.logger = settings_cl.logger
		cl.SetTimeout(settings_cl.timeout)
	}

	rsp, err := cl.Request(request_data, service, endpoint)

	if err != nil {
		return rsp, err
	}

	return rsp, nil
}
