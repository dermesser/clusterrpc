package clusterrpc

import (
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
	call_channel chan *asyncRequest
	qlength      uint32

	logger   *log.Logger
	loglevel LOGLEVEL_T
	client   *Client
}

/*
Create an asynchronous client. An AsyncClient is also called using Request(), but it
queues the request (in a channel with the queue length qlength). The requests
themselves are sent synchronously (REQ/REP), but the initial Request() function returns immediately
if the channel queue is not full yet. The queuing avoids a too high CPU use; higher parallelism
can be achieved by using multiple AsyncClients.

client_name is an arbitrary name that can be used to identify this client at the server (e.g.
in logs)
*/
func NewAsyncClient(client_name, raddr string, rport, qlength uint32) (*AsyncClient, error) {

	cl := new(AsyncClient)
	cl.logger = log.New(os.Stderr, "clusterrpc.AsyncClient "+client_name+": ", log.Lmicroseconds)
	cl.loglevel = LOGLEVEL_ERRORS
	cl.qlength = qlength

	var err error
	cl.client, err = NewClient(client_name, raddr, rport)

	if err != nil {
		cl.logger.Println("Synchronous client constructor returned error:", err.Error())
		return nil, err
	}

	cl.call_channel = make(chan *asyncRequest, qlength)
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
func (cl *AsyncClient) SetLoglevel(ll LOGLEVEL_T) {
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
	cl.call_channel <- &asyncRequest{terminate: true}
}

func (cl *AsyncClient) startThread() {
	for rq := range cl.call_channel {
		if rq.terminate {
			cl.client.Close()
			close(cl.call_channel)
			return
		}

		if cl.loglevel >= LOGLEVEL_WARNINGS && float64(len(cl.call_channel)) > 0.7*float64(cl.qlength) {
			cl.logger.Println("AsyncClient", cl.client.name, "Warning: Queue is fuller than 70% of its capacity!")
		}

		rsp, err := cl.client.Request(rq.data, rq.service, rq.endpoint)

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

	cl.call_channel <- &rq
	return
}
