package server

import (
	"bytes"
	"fmt"
	"github.com/dermesser/clusterrpc/log"
	"github.com/dermesser/clusterrpc/proto"
	"github.com/dermesser/clusterrpc/server/queue"
	"time"

	pb "github.com/gogo/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
)

const BACKEND_ROUTER_PATH string = "inproc://rpc_backend_router"

var MAGIC_READY_STRING []byte = []byte("___ReAdY___")
var MAGIC_STOP_STRING []byte = []byte("___STOPBALANCER___")

const OUTSTANDING_REQUESTS_PER_THREAD uint = 50

type workerRequest struct {
	requestId, clientId, data []byte
}

/*
This file has the internal functions, the actual server; server.go remains
uncluttered and with only public functions.
*/

func (srv *Server) stop() error {
	log.CRPC_log(log.LOGLEVEL_INFO, "Stopping workers...")

	// First stop all worker threads
	for i := uint(0); i < srv.workers; i++ {
		_, err := srv.backend_router.SendMessage(
			newBackendMessage([]byte(fmt.Sprintf("%d", i)),
				newClientMessage([]byte("BOGUS_RQID"), []byte{0xde, 0xad, 0xde, 0xad}, MAGIC_STOP_STRING)).serializeBackendMessage())
		if err != nil {
			log.CRPC_log(log.LOGLEVEL_ERRORS, "Could not send stop message to load balancer, exiting!", err.Error())

			return err
		}
	}

	sock, err := zmq.NewSocket(zmq.REQ)

	log.CRPC_log(log.LOGLEVEL_DEBUG, "Stopping balancer thread...")

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Could not create socket for stopping!")

		return err
	}

	worker_identity := "_x"
	err = sock.SetIdentity(worker_identity)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Could not set identity on socket for stopping, exiting!", err.Error())

		return err
	}

	err = sock.Connect(BACKEND_ROUTER_PATH)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Could not connect to load balancer thread, exiting!", err.Error())

		return err
	}

	_, err = sock.SendMessage(newClientMessage([]byte("__BOGUS_REQ_ID"), []byte("___BOGUS_clientId"), MAGIC_STOP_STRING).serializeClientMessage())

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Could not send stop message to load balancer, exiting!", err.Error())

		return err
	}

	// Wait for ack
	_, err = sock.RecvMessageBytes(0)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Could not send stop message to load balancer, exiting!", err.Error())

		return err
	}

	log.CRPC_log(log.LOGLEVEL_INFO, "Stopped RPC server and workers")

	sock.Close()

	return nil
}

func (srv *Server) handleIncomingRpc(worker_queue *queue.Queue, request_queue *queue.Queue) {
	// The message we're receiving here has this format: [requestId, clientIdentity, "", data].
	msgs, err := srv.frontend_router.RecvMessageBytes(0)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when receiving from frontend:", err.Error())
		return
	}

	message := parseClientMessage(msgs)

	if srv.loadshed_state { // Refuse request.
		request := &proto.RPCRequest{}
		err = request.Unmarshal(message.payload)

		if err != nil {
			log.CRPC_log(log.LOGLEVEL_WARNINGS, "Dropped message; could not decode protobuf:", err.Error())
			return
		}

		srv.sendError(srv.frontend_router, request, proto.RPCResponse_STATUS_LOADSHED,
			&workerRequest{clientId: message.clientId, requestId: message.requestId, data: message.payload})

	} else if worker_id, ok := worker_queue.Pop().([]byte); ok { // Find worker
		_, err = srv.backend_router.SendMessage(newBackendMessage(worker_id, message).serializeBackendMessage()) // [worker identity, "", request identity, client identity, "", RPCRequest]

		if err != nil {
			if err.(zmq.Errno) != zmq.EHOSTUNREACH {
				log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when sending to backend router:", err.Error())
			} else {
				log.CRPC_log(log.LOGLEVEL_ERRORS, "Could not route message, identity", fmt.Sprintf("%x", message.clientId), ", to frontend")
			}
		}

	} else if uint(request_queue.Len()) < srv.workers*OUTSTANDING_REQUESTS_PER_THREAD { // We're only allowing so many queued requests to prevent from complete overloading
		request_queue.Push(message)

		if request_queue.Len() > int(0.8*float64(srv.workers*OUTSTANDING_REQUESTS_PER_THREAD)) {
			log.CRPC_log(log.LOGLEVEL_WARNINGS, "Queue is now at more than 80% fullness. Consider increasing # of workers: (qlen/cap)",
				request_queue.Len(), srv.workers*OUTSTANDING_REQUESTS_PER_THREAD)

		}
	} else {
		// Maybe just drop silently -- this costs CPU!
		request := &proto.RPCRequest{}
		err = request.Unmarshal(message.payload)

		if err != nil {
			log.CRPC_log(log.LOGLEVEL_WARNINGS, "Dropped message; no available workers, queue full")
			return
		}

		srv.sendError(srv.frontend_router, request, proto.RPCResponse_STATUS_OVERLOADED_RETRY,
			&workerRequest{clientId: message.clientId, requestId: message.requestId, data: message.payload})
	}

}

// Returns false if the server loop should be stopped
func (srv *Server) handleWorkerResponse(worker_queue *queue.Queue, request_queue *queue.Queue) bool {
	msgs, err := srv.backend_router.RecvMessageBytes(0) // [worker identity, "", requestId, client identity, "", RPCResponse]

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when receiving from frontend:", err.Error())
		return true
	}

	message := parseBackendMessage(msgs)

	// the data frame is MAGIC_READY_STRING when a worker joins, and MAGIC_STOP_STRING
	// if the app asks to stop
	if bytes.Equal(message.message.payload, MAGIC_READY_STRING) {

		worker_queue.Push(message.workerId)

	} else if bytes.Equal(message.message.payload, MAGIC_STOP_STRING) {

		log.CRPC_log(log.LOGLEVEL_INFO, "Stopped balancer...")

		// Send ack
		_, err = srv.backend_router.SendMessage(message.workerId, "", "DONE")

		if err != nil {
			log.CRPC_log(log.LOGLEVEL_ERRORS, "Couldn't send response to STOP message:", err.Error())
		}
		return false

	} else {
		worker_queue.Push(message.workerId)
		_, err := srv.frontend_router.SendMessage(message.message.serializeClientMessage()) // [request identity, client identity, "", RPCResponse]

		if err != nil {
			if err.(zmq.Errno) != zmq.EHOSTUNREACH {
				log.CRPC_log(log.LOGLEVEL_WARNINGS, "Error when sending to backend router:", err.Error())
			} else if err.(zmq.Errno) == zmq.EHOSTUNREACH {
				// routing is mandatory.
				// Fails when the client has already disconnected
				log.CRPC_log(log.LOGLEVEL_WARNINGS, "Could not route message, worker identity", fmt.Sprintf("%x", message.workerId), "to frontend")
			}
		}
	}

	// Now that we have a new free worker, let's see if there's work in the queue...
	if request_queue.Len() > 0 && worker_queue.Len() > 0 {
		request_message := request_queue.Pop().(clientMessage)
		worker_id := worker_queue.Pop().([]byte)
		_, err := srv.backend_router.SendMessage(newBackendMessage(worker_id, request_message).serializeBackendMessage())
		if err != nil {
			log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when sending to backend router:", err.Error())
		}
	}
	return true
}

/*
Load balancer using the least used worker: We have a list (queue) of backend worker identities;
a backend is queued when it sends a response, and dequeued when it is sent a client request.

Additionally, there's a request queue for the case that there are no workers available at the moment.
This queue is consulted every time a worker has completed a request, which results in a relatively
good resource efficiency.
*/
func (srv *Server) loadbalance() {
	srv.lblock.Lock()
	defer srv.lblock.Unlock()

	// Queue of []byte
	worker_queue := queue.NewQueue(int(srv.workers))

	// request_queue is for incoming requests that find no available worker immediately.
	// We're allowing a backlog of 50 outstanding requests per task; over that, we're dropping
	//
	// Queue of [][]byte!
	request_queue := queue.NewQueue(50)

	poller := zmq.NewPoller()
	poller.Add(srv.frontend_router, zmq.POLLIN)
	poller.Add(srv.backend_router, zmq.POLLIN)

	for {
		polled, err := poller.Poll(-1)

		if err != nil {
			log.CRPC_log(log.LOGLEVEL_ERRORS, "Polling error in loadbalancer:", err.Error())
			continue
		} else {
			for _, sock := range polled {
				switch s := sock.Socket; s {
				case srv.frontend_router:
					srv.handleIncomingRpc(&worker_queue, &request_queue)
				case srv.backend_router:
					if !srv.handleWorkerResponse(&worker_queue, &request_queue) {
						return
					}
				}
			}
		}
	}
}

// Start a single worker thread; spawn a goroutine if spawn == true. Otherwise, execute in the current thread.
// This thread will later execute the registered handlers.
func (srv *Server) thread(n uint, spawn bool) error {
	// Yes, we're using a REQ socket for the worker
	// see http://zguide.zeromq.org/page:all#toc72
	sock, err := zmq.NewSocket(zmq.REQ)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Thread", n, "could not create socket, exiting!")

		return err
	}

	worker_identity := fmt.Sprintf("%d", n)
	err = sock.SetIdentity(worker_identity)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Thread", n, "could not set identity, exiting!")

		return err
	}

	err = sock.Connect(BACKEND_ROUTER_PATH)

	if err != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, "Thread", n, "could not connect to backend router, exiting!")

		return err
	}

	sock.SetSndtimeo(srv.timeout)

	if spawn {
		go srv.acceptRequests(sock, worker_identity)
	} else {
		srv.acceptRequests(sock, worker_identity)
	}
	return nil
}

// This function runs in the (few) threads of the RPC server.
func (srv *Server) acceptRequests(sock *zmq.Socket, worker_identity string) error {

	// Send bogus parts so we have the correct number of frames in this message. (doesn't impact performance)
	sock.SendMessage(newClientMessage([]byte("__BOGUS_REQ_ID"), []byte("___BOGUS_clientId"), MAGIC_READY_STRING).serializeClientMessage())

	for {
		// We're getting here the following message parts: [requestId, client identity, "", data]
		msgs, err := sock.RecvMessageBytes(0)

		if err == nil {
			message := parseClientMessage(msgs)

			if log.IsLoggingEnabled(log.LOGLEVEL_DEBUG) {
				log.CRPC_log(log.LOGLEVEL_DEBUG, fmt.Sprintf("Worker #%s received message from %x", worker_identity, message.clientId))
			}

			if bytes.Equal(message.payload, MAGIC_STOP_STRING) {
				log.CRPC_log(log.LOGLEVEL_DEBUG, fmt.Sprintf("Worker #%s stopped", worker_identity))

				return nil
			}

			req := workerRequest{clientId: message.clientId, requestId: message.requestId, data: message.payload}
			srv.handleRequest(&req, sock)
		} else {
			if err != nil {
				log.CRPC_log(log.LOGLEVEL_WARNINGS, "Skipped incoming message, error:", err.Error())
			}
			continue
		}

	}
}

// Handle one request.
// clientIdentity is the unique number assigned by ZeroMQ. data is the raw data input from the client.
func (srv *Server) handleRequest(request *workerRequest, sock *zmq.Socket) {

	rqproto := new(proto.RPCRequest)
	pberr := pb.Unmarshal(request.data, rqproto)

	if pberr != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, fmt.Sprintf("[%x/_/_] PB unmarshaling error: %s", request.clientId, pberr.Error()))
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_SERVER_ERROR, request)
		return
	}

	caller_id := rqproto.GetCallerId()

	// It is already too late... we can discard this request
	if rqproto.GetDeadline() > 0 && time.Now().Unix() > rqproto.GetDeadline() {
		delta := time.Now().Unix() - rqproto.GetDeadline()

		log.CRPC_log(log.LOGLEVEL_WARNINGS, fmt.Sprintf("[%x/%s/%s] Timeout occurred, deadline was %d (%d s)",
			request.clientId, caller_id, rqproto.GetRpcId(), rqproto.GetDeadline(), delta))

		// Sending this to get the REQ socket in the right state
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_MISSED_DEADLINE, request)
		return
	}

	handler := srv.findHandler(rqproto.GetSrvc(), rqproto.GetProcedure())

	if handler == nil {
		log.CRPC_log(log.LOGLEVEL_WARNINGS,
			fmt.Sprintf("[%x/%s/%s] NOT_FOUND response to request for endpoint %s",
				request.clientId, caller_id, rqproto.GetRpcId(), rqproto.GetSrvc()+"."+rqproto.GetProcedure()))
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_NOT_FOUND, request)
		return
	}

	if log.IsLoggingEnabled(log.LOGLEVEL_DEBUG) {
		log.CRPC_log(log.LOGLEVEL_DEBUG,
			fmt.Sprintf("[%x/%s/%s] Calling endpoint %s.%s...",
				request.clientId, caller_id, rqproto.GetRpcId(), rqproto.GetSrvc(), rqproto.GetProcedure()))
	}

	cx := srv.newContext(rqproto, srv.rpclogger)

	// Actual invocation!!
	handler(cx)

	rpproto := cx.toRPCResponse()
	rpproto.RpcId = rqproto.RpcId

	response_serialized, pberr := rpproto.Marshal()

	if pberr != nil {
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_SERVER_ERROR, request)

		log.CRPC_log(log.LOGLEVEL_ERRORS,
			fmt.Sprintf("[%x/%s/%s] Error when serializing RPCResponse: %s",
				request.clientId, caller_id, rqproto.GetRpcId(), pberr.Error()))

	} else {

		_, err := sock.SendMessage(newClientMessage(request.requestId, request.clientId, response_serialized).serializeClientMessage())

		if err != nil {
			log.CRPC_log(log.LOGLEVEL_WARNINGS,
				fmt.Sprintf("[%x/%s/%s] Error when sending response; %s",
					request.clientId, caller_id, rqproto.GetRpcId(), err.Error()))

			return
		}

		if log.IsLoggingEnabled(log.LOGLEVEL_DEBUG) {
			log.CRPC_log(log.LOGLEVEL_DEBUG, fmt.Sprintf("[%x/%s/%s] Sent response.", request.clientId, caller_id, rqproto.GetRpcId()))
		}

	}
}

// "one-shot" -- doesn't catch Write() errors. But needs a lot of context
func (srv *Server) sendError(sock *zmq.Socket, rq *proto.RPCRequest, s proto.RPCResponse_Status, request *workerRequest) {
	// The context functions do most of the work for us.
	tmp_ctx := srv.newContext(rq, nil)
	tmp_ctx.Fail(s.String())

	response := tmp_ctx.toRPCResponse()
	response.RpcId = rq.RpcId
	response.ResponseStatus = s.Enum()

	buf, err := pb.Marshal(response)

	if err != nil {
		return // Let the client time out. We can't do anything (although this isn't supposed to happen)
	}

	sock.SendMessage(newClientMessage(request.requestId, request.clientId, buf).serializeClientMessage())
}
