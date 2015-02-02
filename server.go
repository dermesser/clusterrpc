package clusterrpc

import (
	"clusterrpc/proto"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"time"

	pb "code.google.com/p/goprotobuf/proto"
)

/*
Handles incoming requests and registering of handler functions.
*/
type Server struct {
	sock     *net.TCPListener
	services map[string]*service
	logger   *log.Logger
	// The timeout only applies on client connections (R/W), not the Listener
	timeout  time.Duration
	loglevel LOGLEVEL_T
}

/*
Type of a function that is called when the corresponding endpoint is requested.
*/
type Endpoint (func(*Context))

type service struct {
	endpoints map[string]Endpoint
}

/*
Create server listening on the specified laddr:port. There is usually only one server listening
per process (though it is entirely possible to use multiple ones)
*/
func NewServer(laddr string, port int) (srv *Server) {
	srv = new(Server)
	srv.services = make(map[string]*service)
	srv.logger = log.New(os.Stderr, "clusterrpc.Server: ", log.Lmicroseconds)
	srv.loglevel = LOGLEVEL_WARNINGS

	addr := new(net.TCPAddr)
	addr.IP = net.ParseIP(laddr)
	addr.Port = port

	var err error
	srv.sock, err = net.ListenTCP("tcp", addr)

	if err != nil {
		srv = nil
	}

	return
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
func (srv *Server) SetClientRWTimeout(d time.Duration) {
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
			srv.logger.Println("Trying to register existing endpoint: ", svc+"."+endpoint)
		}
		err = errors.New("Endpoint already registered; not overwritten")
		return
	}

	if srv.loglevel >= LOGLEVEL_DEBUG {
		srv.logger.Println("Registered endpoint: ", svc+"."+endpoint)
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

// Accept loop. Spawns goroutines for requests. The returned error is guaranteed to be nil or a net.Error value
func (srv *Server) AcceptRequests() error {

	for true {
		conn, err := srv.sock.AcceptTCP()

		if srv.loglevel >= LOGLEVEL_INFO {
			srv.logger.Println("Accepted connection from", conn.RemoteAddr().String())
		}
		if err == nil {
			go srv.handleRequest(conn)
		} else if err.(net.Error).Temporary() || err.(net.Error).Timeout() {
			continue
		} else {
			return err
		}

	}
	return nil
}

// Examine incoming requests, act upon them until the connection is closed.
// TODO Maybe break this up a little bit?
func (srv *Server) handleRequest(conn *net.TCPConn) {
	// Handle requests on one connection until EOF breaks the loop
	var counter int32 = 0
	for true {
		if srv.timeout > 0 {
			conn.SetDeadline(time.Now().Add(srv.timeout))
		}
		// Read one record
		request, err := readSizePrefixedMessage(conn)

		if err != nil {
			if srv.loglevel >= LOGLEVEL_WARNINGS && err.Error() != "EOF" {
				srv.logger.Println("Network error on reading from accepted connection:", err.Error(), conn.RemoteAddr().String())
			} else if srv.loglevel >= LOGLEVEL_INFO {
				srv.logger.Println("Received EOF from client, closing connection", conn.RemoteAddr().String())
			}
			conn.Close()
			return
		} else {
			if srv.loglevel >= LOGLEVEL_DEBUG {
				srv.logger.Println("Received request number", counter, "of this connection")
			}
			counter++
		}

		rqproto := proto.RPCRequest{}
		pberr := pb.Unmarshal(request, &rqproto)

		if pberr != nil {
			if srv.loglevel >= LOGLEVEL_ERRORS {
				srv.logger.Println("PB unmarshaling error:", pberr.Error())
			}
			conn.Close()
			return
		}

		// It is too late... we can discard this request
		if rqproto.GetDeadline() > uint64(time.Now().Unix()) {
			if srv.loglevel >= LOGLEVEL_WARNINGS {
				srv.logger.Println("Timeout occurred, deadline:", rqproto.GetDeadline())
			}
			srv.sendError(conn, rqproto, proto.RPCResponse_STATUS_TIMEOUT)
			return
		}

		// Find matching endpoint
		srvc, srvc_found := srv.services[rqproto.GetSrvc()]

		if !srvc_found {
			if srv.loglevel >= LOGLEVEL_WARNINGS {
				srv.logger.Println("NOT_FOUND response to request for service", rqproto.GetSrvc())
			}
			srv.sendError(conn, rqproto, proto.RPCResponse_STATUS_NOT_FOUND)
			return
		} else {
			if handler, endpoint_found := srvc.endpoints[rqproto.GetProcedure()]; !endpoint_found {
				if srv.loglevel >= LOGLEVEL_WARNINGS {
					srv.logger.Println("NOT_FOUND response to request for endpoint", rqproto.GetSrvc()+"."+rqproto.GetProcedure())
				}
				srv.sendError(conn, rqproto, proto.RPCResponse_STATUS_NOT_FOUND)
				return
			} else {
				cx := NewContext([]byte(rqproto.GetData()))
				handler(cx)

				rpproto := srv.contextToRPCResponse(cx)
				rpproto.SequenceNumber = pb.Uint64(rqproto.GetSequenceNumber())

				response_serialized, pberr := protoToLengthPrefixed(&rpproto)

				if pberr != nil {
					srv.sendError(conn, rqproto, proto.RPCResponse_STATUS_SERVER_ERROR)

					if srv.loglevel >= LOGLEVEL_ERRORS {
						srv.logger.Println(rqproto.GetSequenceNumber(), "Error when serializing RPCResponse:", pberr.Error())
					}

					if rqproto.GetConnectionClose() {
						conn.Close()
					}
				} else {
					if srv.timeout > 0 {
						conn.SetDeadline(time.Now().Add(srv.timeout))
					}
					n, werr := conn.Write(response_serialized)

					if werr != nil && (werr.(net.Error).Temporary() || werr.(net.Error).Timeout()) {
						if srv.loglevel >= LOGLEVEL_WARNINGS {
							srv.logger.Println(rqproto.GetSequenceNumber(), "Timeout or temporary error, retrying twice")
						}
						i := 0

						for i < 2 && (werr.(net.Error).Timeout() || werr.(net.Error).Temporary()) {
							if srv.timeout > 0 {
								conn.SetDeadline(time.Now().Add(srv.timeout))
							}
							n, werr = conn.Write(response_serialized)

							if n == len(response_serialized) && werr == nil {
								break
							}

							i++
						}
						if i == 2 {
							if srv.loglevel >= LOGLEVEL_ERRORS {
								srv.logger.Println(rqproto.GetSequenceNumber(), "Couldn't send message in three attempts, closing connection")
							}
							conn.Close()
							return
						}
					} else if werr != nil {
						if srv.loglevel >= LOGLEVEL_WARNINGS {
							srv.logger.Println(rqproto.GetSequenceNumber(), "Error during sending: ", werr.Error())
						}
						conn.Close()
						return
					} else if n < len(response_serialized) {
						if srv.loglevel >= LOGLEVEL_WARNINGS {
							srv.logger.Println(rqproto.GetSequenceNumber(), "Couldn't send whole message:", n, "out of", len(response_serialized))
						}
					}

					if rqproto.GetConnectionClose() {
						conn.Close()
						return
					}
					if srv.loglevel >= LOGLEVEL_DEBUG {
						srv.logger.Println(rqproto.GetSequenceNumber(), "Sent response to", rqproto.GetSequenceNumber())
					}
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
func (srv *Server) sendError(c *net.TCPConn, rq proto.RPCRequest, s proto.RPCResponse_Status) {
	response := proto.RPCResponse{}
	response.SequenceNumber = rq.SequenceNumber

	response.ResponseStatus = new(proto.RPCResponse_Status)
	*response.ResponseStatus = s

	buf, err := protoToLengthPrefixed(&response)

	if err != nil {
		return // Let the client time out. We can't do anything (although this isn't supposed to happen)
	}

	if srv.timeout > 0 {
		c.SetDeadline(time.Now().Add(srv.timeout))
	}
	// Will fail if we send a STATUS_TIMEOUT message because the client is likely to have closed the
	// connection by now.
	c.Write(buf)

	if rq.GetConnectionClose() {
		c.Close()
	}
}
