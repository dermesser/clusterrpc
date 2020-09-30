package server

import (
	"errors"
	"fmt"
	"github.com/dermesser/clusterrpc/log"
	smgr "github.com/dermesser/clusterrpc/securitymanager"
	golog "log"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
)

/*
Handles incoming requests and registering of handler functions.
*/
type Server struct {
	// Router receives new requests, dealer distributes them between the threads
	frontend_router, backend_router *zmq.Socket
	services                        map[string]*service
	// The timeout only applies on client connections (R/W), not the Listener
	timeout      time.Duration
	workers      uint
	machine_name string
	// Respond "no" to healthchecks
	lameduck_state bool
	// Do not accept requests anymore
	loadshed_state bool

	lblock    sync.Mutex
	rpclogger *golog.Logger
}

/*
Type of a function that is called when the corresponding endpoint is requested.
*/
type Handler (func(*Context))

type service struct {
	endpoints map[string]Handler
}

/*
Create server listening on the specified laddr:port. laddr has to be "*" or an IP address, names
do not work. There is usually only one server listening per process
(though it is possible to use multiple servers on different ports, of course)

worker_threads is the number of workers; however, there are (additionally) at least one load-balancing thread
and one ZeroMQ networking thread.

security_manager adds CURVE and IP "authentication" security to the server. If it's nil, do
not add security.

Use the setter functions described below before calling Start(), otherwise they might
be ignored.

*/
func NewServer(host string, port uint, threads uint, security_manager *smgr.ServerSecurityManager) (*Server, error) {
	return newServer([]string{fmt.Sprintf("tcp://%s:%d", host, port)},
		threads,
		security_manager)
}

func NewIPCServer(path string, threads uint, security_manager *smgr.ServerSecurityManager) (*Server, error) {
	return newServer([]string{fmt.Sprintf("ipc://%s", path)}, threads, security_manager)
}

func newServer(bindurls []string, worker_threads uint, security_manager *smgr.ServerSecurityManager) (*Server, error) {
	srv := new(Server)
	srv.services = make(map[string]*service)
	srv.timeout = time.Second * 3

	if worker_threads <= 0 {
		worker_threads = 1
	}

	srv.workers = worker_threads

	srv.RegisterHandler("__CLUSTERRPC", "Health", makeHealthHandler(&srv.lameduck_state))
	srv.RegisterHandler("__CLUSTERRPC", "Ping", pingHandler)

	var err error
	zmq.SetIpv6(true)

	srv.frontend_router, err = zmq.NewSocket(zmq.ROUTER)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when creating Router socket:", err.Error())
		return nil, err
	}

	srv.frontend_router.SetRouterMandatory(1)
	srv.frontend_router.SetSndtimeo(srv.timeout)
	srv.frontend_router.SetRcvtimeo(srv.timeout)

	err = security_manager.ApplyToServerSocket(srv.frontend_router)

	if err != nil {
		srv.frontend_router.Close()
		return nil, err
	}

	for _, bindurl := range bindurls {
		log.CRPC_log(log.LOGLEVEL_INFO, "Binding frontend to ", bindurl)
		err = srv.frontend_router.Bind(bindurl)
		if err != nil {
			log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when binding Router socket:", err.Error())
			srv.frontend_router.Close()
			return nil, err
		}

	}

	srv.backend_router, err = zmq.NewSocket(zmq.ROUTER)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when creating backend router socket:", err.Error())
		srv.frontend_router.Close()
		return nil, err
	}

	err = srv.backend_router.Bind(BACKEND_ROUTER_PATH)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when binding backend router socket:", err.Error())
		srv.frontend_router.Close()
		srv.backend_router.Close()
		return nil, err
	}

	srv.backend_router.SetRouterMandatory(1)
	srv.backend_router.SetRcvtimeo(srv.timeout)
	srv.backend_router.SetSndtimeo(srv.timeout)

	go srv.loadbalance()

	return srv, nil
}

/*
Starts worker threads. Returns an error if any thread couldn't set up its socket,
otherwise nil. The error is logged at any LOGLEVEL.
*/
func (srv *Server) Start() error {

	var i uint
	for i = 0; i < srv.workers-1; i++ {
		err := srv.thread(i, true)

		if err != nil {
			return err
		}
	}
	return srv.thread(srv.workers-1, false)
}

// Connect to loadbalancer thread and send special stop message.
// Does not close sockets etc.
func (srv *Server) Stop() error {
	return srv.stop()
}

// Close internal sockets. The server may not be used after calling Close().
func (srv *Server) Close() {
	srv.frontend_router.Close()
	srv.backend_router.Close()
}

/*
Set timeout for the routers used by the loadbalancer (the worker sockets don't really need a timeout
because they're communicating via inproc://)
*/
func (srv *Server) SetTimeout(d time.Duration) {
	srv.timeout = d

	srv.backend_router.SetRcvtimeo(srv.timeout)
	srv.backend_router.SetSndtimeo(srv.timeout)
	srv.frontend_router.SetSndtimeo(srv.timeout)
	srv.frontend_router.SetRcvtimeo(srv.timeout)
}

// Set the machine name as shown in traces (os.Hostname() can be used to obtain the DNS name)
func (srv *Server) SetMachineName(name string) {
	srv.machine_name = name
}

/*
Log all RPCs made by this client to this logging device; either as hex/raw strings or protobuf strings.
*/
func (cl *Server) SetRPCLogger(l *golog.Logger) {
	cl.rpclogger = l
}

/*
Add a new endpoint (i.e. a handler); svc is the "namespace" in which to register the handler,
endpoint the name with which the handler can be identified from the outside. The service
is created implicitly

err is not nil if the endpoint is already registered.
*/
func (srv *Server) RegisterHandler(svc, endpoint string, handler Handler) (err error) {
	_, ok := srv.services[svc]

	if !ok {
		srv.services[svc] = new(service)
		srv.services[svc].endpoints = make(map[string]Handler)
	} else if _, ok = srv.services[svc].endpoints[endpoint]; ok {
		log.CRPC_log(log.LOGLEVEL_WARNINGS, "Trying to register existing endpoint:", svc+"."+endpoint)
		err = errors.New("Endpoint already registered; not overwritten")
		return
	}

	log.CRPC_log(log.LOGLEVEL_INFO, "Registered endpoint:", svc+"."+endpoint)

	srv.services[svc].endpoints[endpoint] = handler
	err = nil
	return
}

/*
Removes an endpoint from the set of served endpoints.

Returns an error value with a description if the endpoint doesn't exist.
*/
func (srv *Server) UnregisterHandler(svc, endpoint string) (err error) {

	err = nil
	_, ok := srv.services[svc]

	if !ok {
		log.CRPC_log(log.LOGLEVEL_WARNINGS, "Trying to unregister non-existing endpoint: ", svc+"."+endpoint)

		err = errors.New("No such service")
		return
	} else if _, ok = srv.services[svc].endpoints[endpoint]; !ok {
		log.CRPC_log(log.LOGLEVEL_WARNINGS, "Trying to unregister non-existing endpoint: ", svc+"."+endpoint)

		err = errors.New("No such endpoint")
		return
	} else {
		log.CRPC_log(log.LOGLEVEL_INFO, "Unregistered endpoint: ", svc+"."+endpoint)

		delete(srv.services[svc].endpoints, endpoint)
	}

	return
}

// Returns a handler, or nil if none was found.
func (srv *Server) findHandler(service, endpoint string) Handler {
	if service, ok := srv.services[service]; ok {
		if handler, ok := service.endpoints[endpoint]; ok {
			return handler
		} else {
			return nil
		}
	} else {
		return nil
	}
}

/*
A server that is in lameduck mode will respond negatively to health checks
but continue serving requests.
*/
func (srv *Server) SetLameduck(lameduck bool) {
	srv.lameduck_state = lameduck
}

/*
A server in loadshed mode will refuse any requests immediately.
*/
func (srv *Server) SetLoadshed(loadshed bool) {
	srv.loadshed_state = loadshed
}
