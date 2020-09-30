package client

import (
	"github.com/dermesser/clusterrpc/log"
	smgr "github.com/dermesser/clusterrpc/securitymanager"
	"time"
)

type Callback func([]byte, error)

type asyncRequest struct {
	callback          Callback
	data              []byte
	service, endpoint string
	// If this is set, terminate client and clean up
	terminate bool
}

type AsyncClient struct {
	request_queue chan *asyncRequest
	qlength       uint

	client Client
}

/*
Create an asynchronous client. An AsyncClient is also called using Request(), but it
queues the request (in a buffered channel with the length queue_length). The requests
themselves are sent synchronously (REQ/REP), but the Request() function returns immediately
if the channel queue is not full yet. The queuing avoids a too high CPU use on both server and client;
higher parallelism can simply be achieved by using multiple AsyncClients.

client_name is an arbitrary name that can be used to identify this client at the server (e.g.
in logs)
*/
func NewAsyncClient(client_name string, addr PeerAddress, queue_length uint,
	security_manager *smgr.ClientSecurityManager) (*AsyncClient, error) {

	cl := new(AsyncClient)
	cl.qlength = queue_length

	var err error
	ch, err := NewRpcChannel(security_manager)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Couldn't create channel:", err)
		return nil, err
	}

	err = ch.Connect(addr)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Couldn't connect to peer:", err)
		return nil, err
	}

	cl.client = NewClient(client_name, ch)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Synchronous client constructor returned error:", err.Error())
		return nil, err
	}

	cl.request_queue = make(chan *asyncRequest, queue_length)
	go cl.startThread()

	return cl, nil
}

/*
Set timeout for writes.
*/
func (cl *AsyncClient) SetTimeout(d time.Duration) {
	cl.client.SetTimeout(d, true /* propagate */)
}

func (cl *AsyncClient) Close() {
	cl.request_queue <- &asyncRequest{terminate: true}
}

func (cl *AsyncClient) startThread() {
	for rq := range cl.request_queue {
		if rq.terminate {
			cl.client.Destroy()
			close(cl.request_queue)
			return
		}

		if float64(len(cl.request_queue)) > 0.7*float64(cl.qlength) {
			log.CRPC_log(log.LOGLEVEL_WARNINGS, "AsyncClient", cl.client.name, "Warning: Queue is fuller than 70% of its capacity!")
		}

		rsp, err := cl.client.Request(rq.data, rq.service, rq.endpoint, nil)

		rq.callback(rsp, err)
	}
}

func (cl *AsyncClient) Request(data []byte, service, endpoint string, cb Callback) {
	rq := asyncRequest{}
	rq.callback = cb
	rq.data = data
	rq.endpoint = endpoint
	rq.service = service
	rq.terminate = false

	cl.request_queue <- &rq
	return
}
