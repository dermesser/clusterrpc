/*
Clusterrpc is a server/client library for doing RPC. It uses protocol buffers for transport,
the user payload data has however no defined format, but is simply a sequence of bytes (which
are recommended to be serialized from and to protocol buffers, however).

Clusterrpc works with Services and Endpoints. One Server (i.e. a process with a socket listening on
a port) can serve several Services ("scopes") with each Service having multiple Endpoints (procedures).
An endpoint is essentially a registered handler function.

E.g.:

* Service LookupService

** Endpoint Get (LookupService.Get)

** Endpoint Put (LookupService.Put)

** Endpoint Delete (LookupService.Delete)

*/
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
	timeout time.Duration
}

/*
Type of a function that is called when the corresponding endpoint is requested.
*/
type Endpoint (func([]byte) ([]byte, error))

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
		err = errors.New("Endpoint already registered; not overwritten")
		return
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
		err = errors.New("No such service")
		return
	} else if _, ok = srv.services[svc].endpoints[endpoint]; !ok {
		err = errors.New("No such endpoint")
		return
	} else {
		delete(srv.services[svc].endpoints, endpoint)
	}

	return
}

// Accept loop. Spawns goroutines for requests
func (srv *Server) AcceptRequests() error {

	for true {
		conn, err := srv.sock.AcceptTCP()

		if err == nil {
			srv.logger.Println("Received request.")
			go srv.handleRequest(conn)
		} else if err.(net.Error).Temporary() || err.(net.Error).Timeout() {
			continue
		} else {
			return err
		}

	}
	return nil
}

// Examine the request, call the handler, send response
func (srv *Server) handleRequest(conn *net.TCPConn) {
	// Read one record

	if srv.timeout > 0 {
		conn.SetDeadline(time.Now().Add(srv.timeout))
	}
	request, err := readSizePrefixedMessage(conn)

	if err != nil {
		srv.logger.Println("Network error on reading from accepted connection:", err.Error())
		return
	}

	rqproto := proto.RPCRequest{}
	pb.Unmarshal(request, &rqproto)

	// Find matching endpoint
	srvc, srvc_found := srv.services[*rqproto.Srvc]

	if !srvc_found {
		srv.sendError(conn, rqproto, proto.RPCResponse_STATUS_NOT_FOUND)
		return
	} else {
		if handler, endpoint_found := srvc.endpoints[rqproto.GetProcedure()]; !endpoint_found {
			srv.sendError(conn, rqproto, proto.RPCResponse_STATUS_NOT_FOUND)
			return
		} else {
			response_data, err := handler([]byte(rqproto.GetData()))

			rpproto := proto.RPCResponse{}
			rpproto.SequenceNumber = rqproto.SequenceNumber
			rpproto.ResponseStatus = new(proto.RPCResponse_Status)

			if err == nil {
				*rpproto.ResponseStatus = proto.RPCResponse_STATUS_OK
			} else {
				*rpproto.ResponseStatus = proto.RPCResponse_STATUS_NOT_OK
			}

			rpproto.ResponseData = pb.String(string(response_data))
			srv.logger.Println("Length of response:", len(rpproto.GetResponseData()))

			if err != nil {
				rpproto.ErrorMessage = pb.String(err.Error())
			}

			response_serialized, pberr := protoToLengthPrefixed(&rpproto)

			if pberr != nil {
				srv.sendError(conn, rqproto, proto.RPCResponse_STATUS_SERVER_ERROR)
				srv.logger.Println("Error when serializing RPCResponse:", pberr.Error())

				if rqproto.GetConnectionClose() {
					conn.Close()
				}
			} else {
				if srv.timeout > 0 {
					conn.SetDeadline(time.Now().Add(srv.timeout))
				}
				n, werr := conn.Write(response_serialized)

				if werr != nil && (werr.(net.Error).Temporary() || werr.(net.Error).Timeout()) {
					srv.logger.Println("Timeout or temporary error, retrying")
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
				} else if werr != nil {
					srv.logger.Println("Error during sending: ", werr.Error())
					conn.Close()
					return
				} else if n < len(response_serialized) {
					srv.logger.Println("Couldn't send whole message.")
				}

				if rqproto.GetConnectionClose() {
					conn.Close()
				}
				srv.logger.Println("Sent response")
			}
		}
	}

}

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
	c.Write(buf)

	if rq.GetConnectionClose() {
		c.Close()
	}
}
