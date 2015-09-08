package server

import (
	"clusterrpc"
	"sync"

	smgr "clusterrpc/securitymanager"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
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
	logger                          *log.Logger
	// The timeout only applies on client connections (R/W), not the Listener
	timeout      time.Duration
	loglevel     clusterrpc.LOGLEVEL_T
	n_threads    uint
	machine_name string
	// Respond "no" to healthchecks
	lameduck_state bool
	// Do not accept requests anymore
	loadshed_state bool

	lblock sync.Mutex
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
func NewServer(laddr string, port uint, worker_threads uint, loglevel clusterrpc.LOGLEVEL_T,
	security_manager *smgr.ServerSecurityManager) (*Server, error) {

	srv := new(Server)
	srv.services = make(map[string]*service)
	srv.logger = log.New(os.Stderr, "clusterrpc.Server: ", log.LstdFlags|log.Lmicroseconds)
	srv.loglevel = loglevel
	srv.n_threads = worker_threads
	srv.timeout = time.Second * 3

	if worker_threads <= 0 {
		if srv.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
			srv.logger.Println("Number of threads must be 1 or higher")
		}
		return nil, errors.New("Number of threads must be 1 or higher")
	}

	srv.RegisterHandler("__CLUSTERRPC", "Health", makeHealthHandler(&srv.lameduck_state))

	var err error
	zmq.SetIpv6(true)

	srv.frontend_router, err = zmq.NewSocket(zmq.ROUTER)

	if err != nil {
		if srv.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
			srv.logger.Println("Error when creating Router socket:", err.Error())
		}
		return nil, err
	}

	if err != nil {
		if srv.loglevel >= clusterrpc.LOGLEVEL_WARNINGS {
			srv.logger.Println("Could not enable IPv6 on frontend router:", err.Error())
		}
		return nil, err
	}

	srv.frontend_router.SetRouterMandatory(1)
	srv.frontend_router.SetSndtimeo(srv.timeout)
	srv.frontend_router.SetRcvtimeo(srv.timeout)

	if srv.loglevel >= clusterrpc.LOGLEVEL_INFO {
		srv.logger.Println("Binding frontend to TCP address", fmt.Sprintf("tcp://%s:%d", laddr, port))
	}

	err = security_manager.ApplyToServerSocket(srv.frontend_router)

	if err != nil {
		return nil, err
	}

	err = srv.frontend_router.Bind(fmt.Sprintf("tcp://%s:%d", laddr, port))

	if err != nil {
		if srv.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
			srv.logger.Println("Error when binding Router socket:", err.Error())
		}
		return nil, err
	}

	srv.backend_router, err = zmq.NewSocket(zmq.ROUTER)

	if err != nil {
		if srv.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
			srv.logger.Println("Error when creating backend router socket:", err.Error())
		}
		srv = nil
		return nil, err
	}

	err = srv.backend_router.Bind(BACKEND_ROUTER_PATH)

	if err != nil {
		if srv.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
			srv.logger.Println("Error when binding backend router socket:", err.Error())
		}
		srv = nil
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
	for i = 0; i < srv.n_threads-1; i++ {
		err := srv.thread(i, true)

		if err != nil {
			return err
		}
	}
	return srv.thread(srv.n_threads-1, false)
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
	zmq.Term()
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

/*
Disable logging.
*/
func (srv *Server) DisableLogging() {
	srv.logger = log.New(ioutil.Discard, srv.logger.Prefix(), srv.logger.Flags())
}

/*
Set loglevel of this server.
*/
func (srv *Server) SetLoglevel(l clusterrpc.LOGLEVEL_T) {
	srv.loglevel = l
}

// Set the machine name as shown in traces (os.Hostname() can be used to obtain the DNS name)
func (srv *Server) SetMachineName(name string) {
	srv.machine_name = name
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
		if srv.loglevel >= clusterrpc.LOGLEVEL_WARNINGS {
			srv.logger.Println("Trying to register existing endpoint:", svc+"."+endpoint)
		}
		err = errors.New("Endpoint already registered; not overwritten")
		return
	}

	if srv.loglevel >= clusterrpc.LOGLEVEL_DEBUG {
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
func (srv *Server) UnregisterHandler(svc, endpoint string) (err error) {

	err = nil
	_, ok := srv.services[svc]

	if !ok {
		if srv.loglevel >= clusterrpc.LOGLEVEL_WARNINGS {
			srv.logger.Println("Trying to unregister non-existing endpoint: ", svc+"."+endpoint)
		}
		err = errors.New("No such service")
		return
	} else if _, ok = srv.services[svc].endpoints[endpoint]; !ok {
		if srv.loglevel >= clusterrpc.LOGLEVEL_WARNINGS {
			srv.logger.Println("Trying to unregister non-existing endpoint: ", svc+"."+endpoint)
		}
		err = errors.New("No such endpoint")
		return
	} else {
		if srv.loglevel >= clusterrpc.LOGLEVEL_DEBUG {
			srv.logger.Println("Registered endpoint: ", svc+"."+endpoint)
		}
		delete(srv.services[svc].endpoints, endpoint)
	}

	return
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
