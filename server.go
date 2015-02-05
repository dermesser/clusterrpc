package clusterrpc

import (
	"clusterrpc/proto"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	pb "code.google.com/p/goprotobuf/proto"
	zmq4 "github.com/pebbe/zmq4"
)

const DEALERPATH string = "inproc://rpc_dealer"

/*
Handles incoming requests and registering of handler functions.
*/
type Server struct {
	zmq_context *zmq4.Context
	// Router receives new requests, dealer distributes them between the threads
	router, dealer *zmq4.Socket
	services       map[string]*service
	logger         *log.Logger
	// The timeout only applies on client connections (R/W), not the Listener
	timeout   time.Duration
	loglevel  LOGLEVEL_T
	n_threads int
}

/*
Type of a function that is called when the corresponding endpoint is requested.
*/
type Endpoint (func(*Context))

type service struct {
	endpoints map[string]Endpoint
}

/*
Create server listening on the specified laddr:port. laddr has to be "*" or an IP address, names
do not work. There is usually only one server listening per process
(though it is possible to use multiple servers on different ports, of course)

Use the setter functions described below before calling Start(), otherwise they might
be ignored.

*/
func NewServer(laddr string, port, n_threads int) (srv *Server) {

	srv = new(Server)
	srv.services = make(map[string]*service)
	srv.logger = log.New(os.Stderr, "clusterrpc.Server: ", log.Lmicroseconds)
	srv.loglevel = LOGLEVEL_WARNINGS
	srv.n_threads = n_threads
	srv.timeout = time.Second * 30

	if n_threads <= 0 {
		srv.logger.Println("Number of threads must be 1 or higher")
		return nil
	}

	var err error
	srv.zmq_context, err = zmq4.NewContext()

	if err != nil {
		srv.logger.Println("Error when creating context:", err.Error())
		return nil
	}

	srv.router, err = srv.zmq_context.NewSocket(zmq4.ROUTER)

	if err != nil {
		srv.logger.Println("Error when creating Router socket:", err.Error())
		return nil
	}

	srv.logger.Println("TCP address", fmt.Sprintf("tcp://%s:%d", laddr, port))
	err = srv.router.Bind(fmt.Sprintf("tcp://%s:%d", laddr, port))

	if err != nil {
		srv.logger.Println("Error when binding Router socket:", err.Error())
		return nil
	}

	srv.dealer, err = srv.zmq_context.NewSocket(zmq4.DEALER)

	if err != nil {
		srv.logger.Println("Error when creating Dealer socket:", err.Error())
		srv = nil
		return
	}

	err = srv.dealer.Bind(DEALERPATH)

	if err != nil {
		srv.logger.Println("Error when binding Dealer socket:", err.Error())
		srv = nil
		return
	}

	go zmq4.Proxy(srv.router, srv.dealer, nil)

	return
}

/*
Starts accepting messages. Returns an error if any thread couldn't set up its socket,
otherwise nil. The error is logged at any LOGLEVEL.
*/
func (srv *Server) Start() error {

	for i := 0; i < srv.n_threads-1; i++ {
		err := srv.thread(i, false)

		if err != nil {
			return err
		}
	}
	return srv.thread(srv.n_threads-1, true)
}

func (srv *Server) thread(n int, block bool) error {
	sock, err := srv.zmq_context.NewSocket(zmq4.REP)

	if err != nil {
		srv.logger.Println("Thread", n, "could not create socket, exiting!")
		return err
	}

	err = sock.Connect(DEALERPATH)

	if err != nil {
		srv.logger.Println("Thread", n, "could not connect to Dealer, exiting!")
		return err
	}

	sock.SetSndtimeo(srv.timeout)

	if !block {
		go srv.acceptRequests(sock)
	} else {
		srv.acceptRequests(sock)
	}
	return nil
}

/*
Set logging device.
*/
func (srv *Server) SetLoggingOutput(w io.Writer) {
	srv.logger = log.New(w, srv.logger.Prefix(), srv.logger.Flags())
}

/*
Set logger.
*/
func (srv *Server) SetLogger(l *log.Logger) {
	srv.logger = l
}

/*
Disable logging.
*/
func (srv *Server) DisableLogging() {
	srv.logger = log.New(ioutil.Discard, srv.logger.Prefix(), srv.logger.Flags())
}

/*
Set the timeout that applies to reads and writes on client sockets.
*/
func (srv *Server) SetClientWTimeout(d time.Duration) {
	srv.timeout = d
}

/*
Set loglevel of this server.
*/
func (srv *Server) SetLoglevel(l LOGLEVEL_T) {
	srv.loglevel = l
}

/*
Add a new endpoint (i.e. a handler); svc is the "namespace" in which to register the handler,
endpoint the name with which the handler can be identified from the outside. The service
is created implicitly

err is not nil if the endpoint is already registered.
*/
func (srv *Server) RegisterEndpoint(svc, endpoint string, handler Endpoint) (err error) {
	_, ok := srv.services[svc]

	if !ok {
		srv.services[svc] = new(service)
		srv.services[svc].endpoints = make(map[string]Endpoint)
	} else if _, ok = srv.services[svc].endpoints[endpoint]; ok {
		if srv.loglevel >= LOGLEVEL_WARNINGS {
			srv.logger.Println("Trying to register existing endpoint:", svc+"."+endpoint)
		}
		err = errors.New("Endpoint already registered; not overwritten")
		return
	}

	if srv.loglevel >= LOGLEVEL_DEBUG {
		srv.logger.Println("Registered endpoint:", svc+"."+endpoint)
	}
	srv.services[svc].endpoints[endpoint] = handler
	err = nil
	return
}

/*
Removes an endpoint from the set of served endpoints.

Returns an error value with a description if the endpoint doesn't exist.
*/
func (srv *Server) UnregisterEndpoint(svc, endpoint string) (err error) {

	err = nil
	_, ok := srv.services[svc]

	if !ok {
		if srv.loglevel >= LOGLEVEL_WARNINGS {
			srv.logger.Println("Trying to unregister non-existing endpoint: ", svc+"."+endpoint)
		}
		err = errors.New("No such service")
		return
	} else if _, ok = srv.services[svc].endpoints[endpoint]; !ok {
		if srv.loglevel >= LOGLEVEL_WARNINGS {
			srv.logger.Println("Trying to unregister non-existing endpoint: ", svc+"."+endpoint)
		}
		err = errors.New("No such endpoint")
		return
	} else {
		if srv.loglevel >= LOGLEVEL_DEBUG {
			srv.logger.Println("Registered endpoint: ", svc+"."+endpoint)
		}
		delete(srv.services[svc].endpoints, endpoint)
	}

	return
}

// This function runs in the (few) threads of the RPC server.
func (srv *Server) acceptRequests(sock *zmq4.Socket) error {

	for true {
		msgs, err := sock.RecvMessage(0)

		if srv.loglevel >= LOGLEVEL_INFO {
			srv.logger.Printf("Received message\n")
		}
		if err == nil {
			srv.handleRequest(msgs[0], sock)
		} else {
			if srv.loglevel >= LOGLEVEL_WARNINGS {
				srv.logger.Println("Skipped incoming message, error:", err.Error())
			}
			continue
		}

	}
	return nil
}

// Handle one request.
// request[0] is the identity, request[1] is empty, request[2] is the actual data.
// TODO Maybe break this up a little bit?
func (srv *Server) handleRequest(request string, sock *zmq4.Socket) {

	rqproto := proto.RPCRequest{}
	pberr := pb.Unmarshal([]byte(request), &rqproto)

	if pberr != nil {
		if srv.loglevel >= LOGLEVEL_ERRORS {
			srv.logger.Printf("PB unmarshaling error: %s in request from zmq-id %x", pberr.Error(), request[0])
		}
		// We can't send an error response because we don't even have a sequence number
		sock.SendMessage(request, "", "")
		return
	}

	caller_id := rqproto.GetCallerId()

	// It is too late... we can discard this request
	if rqproto.GetDeadline() > 0 && rqproto.GetDeadline() < time.Now().Unix() {
		if srv.loglevel >= LOGLEVEL_WARNINGS {
			delta := time.Now().Unix() - rqproto.GetDeadline()
			srv.logger.Printf("[%s/%d] Timeout occurred, deadline was %d (%d s)", caller_id, rqproto.GetSequenceNumber(), rqproto.GetDeadline(), delta)
		}
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_TIMEOUT)
		return
	}

	// Find matching endpoint
	srvc, srvc_found := srv.services[rqproto.GetSrvc()]

	if !srvc_found {
		if srv.loglevel >= LOGLEVEL_WARNINGS {
			srv.logger.Printf("[%s/%d] NOT_FOUND response to request for service %s\n", caller_id, rqproto.GetSequenceNumber(), rqproto.GetSrvc())
		}
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_NOT_FOUND)
		return
	} else {
		if handler, endpoint_found := srvc.endpoints[rqproto.GetProcedure()]; !endpoint_found {
			if srv.loglevel >= LOGLEVEL_WARNINGS {
				srv.logger.Printf("[%s/%d] NOT_FOUND response to request for endpoint %s\n", caller_id, rqproto.GetSequenceNumber(), rqproto.GetSrvc()+"."+rqproto.GetProcedure())
			}
			srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_NOT_FOUND)
			return
		} else {
			cx := NewContext([]byte(rqproto.GetData()))
			handler(cx)

			rpproto := srv.contextToRPCResponse(cx)
			rpproto.SequenceNumber = pb.Uint64(rqproto.GetSequenceNumber())

			response_serialized, pberr := pb.Marshal(&rpproto)

			if pberr != nil {
				srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_SERVER_ERROR)

				if srv.loglevel >= LOGLEVEL_ERRORS {
					srv.logger.Printf("[%s/%d] Error when serializing RPCResponse: %s\n", caller_id, rqproto.GetSequenceNumber(), pberr.Error())
				}
			} else {

				_, err := sock.SendMessage(string(response_serialized))

				if err != nil {
					if srv.loglevel >= LOGLEVEL_WARNINGS {
						srv.logger.Printf("[%s/%d] Error when sending response; %s\n", caller_id, rqproto.GetSequenceNumber(), err.Error())
					}
					return
				}

				if srv.loglevel >= LOGLEVEL_DEBUG {
					srv.logger.Printf("[%s/%d] Sent response.\n", caller_id, rqproto.GetSequenceNumber())
				}
			}
		}
	}
}

func (srv *Server) contextToRPCResponse(cx *Context) proto.RPCResponse {
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
		rpproto.RedirPort = pb.Int32(cx.redir_port)
		*rpproto.ResponseStatus = proto.RPCResponse_STATUS_REDIRECT
	}

	return rpproto
}

// "one-shot" -- doesn't catch Write() errors
func (srv *Server) sendError(sock *zmq4.Socket, rq proto.RPCRequest, s proto.RPCResponse_Status) {
	response := proto.RPCResponse{}
	response.SequenceNumber = rq.SequenceNumber

	response.ResponseStatus = new(proto.RPCResponse_Status)
	*response.ResponseStatus = s

	buf, err := pb.Marshal(&response)

	if err != nil {
		return // Let the client time out. We can't do anything (although this isn't supposed to happen)
	}

	// Will fail if we send a STATUS_TIMEOUT message because the client is likely to have closed the
	// connection by now.
	sock.SendMessage(buf)
}
