package client

import (
	"clusterrpc"
	"io"
	"log"
	"os"
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

	logger   *log.Logger
	loglevel clusterrpc.LOGLEVEL_T
	client   *Client
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
func NewAsyncClient(client_name, raddr string, rport, queue_length uint, loglevel clusterrpc.LOGLEVEL_T,
	security_manager *ClientSecurityManager) (*AsyncClient, error) {

	cl := new(AsyncClient)
	cl.logger = log.New(os.Stderr, "clusterrpc.AsyncClient "+client_name+": ", log.Lmicroseconds)
	cl.loglevel = loglevel
	cl.qlength = queue_length

	var err error
	cl.client, err = NewClient(client_name, raddr, rport, cl.loglevel, security_manager)

	if err != nil {
		cl.logger.Println("Synchronous client constructor returned error:", err.Error())
		return nil, err
	}

	cl.request_queue = make(chan *asyncRequest, queue_length)
	go cl.startThread()

	return cl, nil
}

/*
Change the writer to which the client logs operations.
*/
func (cl *AsyncClient) SetLoggingOutput(w io.Writer) {

	cl.logger = log.New(w, cl.logger.Prefix(), cl.logger.Flags())
}

/*
Set the logger of the client to a custom one.
*/
func (cl *AsyncClient) SetLogger(l *log.Logger) {
	cl.logger = l
}

/*
Define which errors/situations to log
*/
func (cl *AsyncClient) SetLoglevel(ll clusterrpc.LOGLEVEL_T) {
	cl.loglevel = ll
	cl.client.loglevel = ll
}

/*
Set timeout for writes.
*/
func (cl *AsyncClient) SetTimeout(d time.Duration) {
	cl.client.SetTimeout(d)
}

func (cl *AsyncClient) Close() {
	cl.request_queue <- &asyncRequest{terminate: true}
}

func (cl *AsyncClient) startThread() {
	for rq := range cl.request_queue {
		if rq.terminate {
			cl.client.Close()
			close(cl.request_queue)
			return
		}

		if cl.loglevel >= clusterrpc.LOGLEVEL_WARNINGS && float64(len(cl.request_queue)) > 0.7*float64(cl.qlength) {
			cl.logger.Println("AsyncClient", cl.client.name, "Warning: Queue is fuller than 70% of its capacity!")
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
