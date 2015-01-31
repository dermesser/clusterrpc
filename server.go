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
	"errors"
	"net"
)

/*
Handles incoming requests and registering of handler functions.
*/
type Server struct {
	sock     *net.TCPListener
	services map[string]*service
}

/*
Type of a function that is called when the corresponding endpoint is requested.
*/
type Endpoint (func([]byte) ([]byte, error))

type service struct {
	endpoints map[string]Endpoint
}

/*
Create server listening on the specified laddr:port.
*/
func NewServer(laddr string, port int) (srv *Server) {
	srv = new(Server)
	srv.services = make(map[string]*service)

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
