package server

import (
	"bytes"
	"clusterrpc/log"
	"clusterrpc/proto"
	"container/list"
	"fmt"
	"time"

	pb "github.com/gogo/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
)

const BACKEND_ROUTER_PATH string = "inproc://rpc_backend_router"

var MAGIC_READY_STRING []byte = []byte("___ReAdY___")
var MAGIC_STOP_STRING []byte = []byte("___STOPBALANCER___")

const OUTSTANDING_REQUESTS_PER_THREAD uint = 50

type workerRequest struct {
	request_id, client_id, data []byte
}

/*
This file has the internal functions, the actual server; server.go remains
uncluttered and with only public functions.
*/

func (srv *Server) stop() error {
	log.CRPC_log(log.LOGLEVEL_INFO, "Stopping workers...")

	// First stop all worker threads
	for i := uint(0); i < srv.n_threads; i++ {
		_, err := srv.backend_router.SendMessage(
			fmt.Sprintf("%d", i), "", []byte{0xde, 0xad, 0xde, 0xad}, "BOGUS_RQID", "", MAGIC_STOP_STRING) // [worker identity, "", client identity, request_id, "", RPCRequest]
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

	_, err = sock.SendMessage("___BOGUS_CLIENT_ID", "__BOGUS_REQ_ID", "", MAGIC_STOP_STRING)

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

	// List of workers that are free -- list of []byte!
	worker_queue := list.New()

	// todo_queue is for incoming requests that find no available worker immediately.
	// We're allowing a backlog of 50 outstanding requests per task; over that, we're dropping
	//
	// List of [][]byte!
	todo_queue := list.New()

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
					// The message we're receiving here has this format: [client_identity, request_id, "", data].
					msgs, err := srv.frontend_router.RecvMessageBytes(0)

					if err != nil {
						log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when receiving from frontend:", err.Error())
						continue
					}
					if len(msgs) != 4 {
						log.CRPC_log(log.LOGLEVEL_ERRORS, "Error: Skipping message with other than 4 frames from frontend router;", len(msgs), "frames received")
						continue
					}

					if srv.loadshed_state { // Refuse request.
						request := &proto.RPCRequest{}
						err = request.Unmarshal(msgs[3])

						if err != nil {
							log.CRPC_log(log.LOGLEVEL_WARNINGS, "Dropped message; could not decode protobuf:", err.Error())
							continue
						}

						srv.sendError(srv.frontend_router, request, proto.RPCResponse_STATUS_LOADSHED,
							&workerRequest{client_id: msgs[0], request_id: msgs[1], data: msgs[3]})

					} else if worker_id := worker_queue.Front(); worker_id != nil { // Find worker
						_, err = srv.backend_router.SendMessage(worker_id.Value.([]byte), "", msgs) // [worker identity, "", client identity, "", RPCRequest]
						worker_queue.Remove(worker_id)

						if err != nil {
							if err.(zmq.Errno) != zmq.EHOSTUNREACH {
								log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when sending to backend router:", err.Error())
							} else {
								log.CRPC_log(log.LOGLEVEL_ERRORS, "Could not route message, identity", fmt.Sprintf("%x", msgs[0]), ", to frontend")
							}
						}

					} else if uint(todo_queue.Len()) < srv.n_threads*OUTSTANDING_REQUESTS_PER_THREAD { // We're only allowing so many queued requests to prevent from complete overloading
						todo_queue.PushBack(msgs)

						if todo_queue.Len() > int(0.8*float64(srv.n_threads*OUTSTANDING_REQUESTS_PER_THREAD)) {
							log.CRPC_log(log.LOGLEVEL_WARNINGS, "Queue is now at more than 80% fullness. Consider increasing # of workers: (qlen/cap)",
								todo_queue.Len(), srv.n_threads*OUTSTANDING_REQUESTS_PER_THREAD)

						}
					} else {
						// Maybe just drop silently -- this costs CPU!
						request := &proto.RPCRequest{}
						err = request.Unmarshal(msgs[3])

						if err != nil {
							log.CRPC_log(log.LOGLEVEL_WARNINGS, "Dropped message; no available workers, queue full")
							continue
						}

						srv.sendError(srv.frontend_router, request, proto.RPCResponse_STATUS_OVERLOADED_RETRY,
							&workerRequest{client_id: msgs[0], request_id: msgs[1], data: msgs[3]})
					}

				case srv.backend_router:
					msgs, err := srv.backend_router.RecvMessageBytes(0) // [worker identity, "", client identity, request_id, "", RPCResponse]

					if err != nil {
						log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when receiving from frontend:", err.Error())
						continue
					}
					if len(msgs) != 6 {
						log.CRPC_log(log.LOGLEVEL_ERRORS, "Error: Skipping message with other than 5 frames from backend;", len(msgs), "frames received")
						continue
					}

					backend_identity := msgs[0]

					// the data frame is MAGIC_READY_STRING when a worker joins, and MAGIC_STOP_STRING
					// if the app asks to stop
					if bytes.Equal(msgs[5], MAGIC_READY_STRING) {

						worker_queue.PushBack(backend_identity)

					} else if bytes.Equal(msgs[5], MAGIC_STOP_STRING) {

						log.CRPC_log(log.LOGLEVEL_INFO, "Stopped balancer...")

						// Send ack
						_, err = srv.backend_router.SendMessage(backend_identity, "", "DONE")

						if err != nil {
							log.CRPC_log(log.LOGLEVEL_ERRORS, "Couldn't send response to STOP message:", err.Error())
						}
						return

					} else {
						worker_queue.PushBack(backend_identity)
						_, err := srv.frontend_router.SendMessage(msgs[2:]) // [client identity, request_id, "", RPCResponse]

						if err != nil {
							if err.(zmq.Errno) != zmq.EHOSTUNREACH {
								log.CRPC_log(log.LOGLEVEL_WARNINGS, "Error when sending to backend router:", err.Error())
							} else if err.(zmq.Errno) == zmq.EHOSTUNREACH {
								// routing is mandatory.
								// Fails when the client has already disconnected
								log.CRPC_log(log.LOGLEVEL_WARNINGS, "Could not route message, worker identity", fmt.Sprintf("%x", msgs[0]), "to frontend")
							}
						}
					}

					// Now that we have a new free worker, let's see if there's work in the queue...
					if todo_queue.Len() > 0 && worker_queue.Len() > 0 {
						request_message := todo_queue.Remove(todo_queue.Front()).([][]byte)
						worker_id := worker_queue.Remove(worker_queue.Front()).([]byte)
						_, err = srv.backend_router.SendMessage(worker_id, "", request_message) // [worker identity, "", client identity, request_id, "", RPCRequest]
						if err != nil {
							log.CRPC_log(log.LOGLEVEL_ERRORS, "Error when sending to backend router:", err.Error())
						}
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
	sock.SendMessage("___BOGUS_CLIENT_ID", "__BOGUS_REQ_ID", "", MAGIC_READY_STRING)

	for {
		// We're getting here the following message parts: [client_identity, request_id, "", data]
		msgs, err := sock.RecvMessageBytes(0)

		if err == nil && len(msgs) == 4 {
			log.CRPC_log(log.LOGLEVEL_DEBUG, fmt.Sprintf("Worker #%s received message from %x", worker_identity, msgs[0]))

			if bytes.Equal(msgs[3], MAGIC_STOP_STRING) {
				log.CRPC_log(log.LOGLEVEL_DEBUG, fmt.Sprintf("Worker #%s stopped", worker_identity))

				return nil
			}

			req := workerRequest{client_id: msgs[0], request_id: msgs[1], data: msgs[3]}
			srv.handleRequest(&req, sock)
		} else {

			if err != nil {
				log.CRPC_log(log.LOGLEVEL_WARNINGS, "Skipped incoming message, error:", err.Error())
			} else if len(msgs) != 4 {
				log.CRPC_log(log.LOGLEVEL_WARNINGS, "Frontend router sent message with != 4 frames")
			}
			continue
		}

	}
	return nil
}

// Handle one request.
// client_identity is the unique number assigned by ZeroMQ. data is the raw data input from the client.
func (srv *Server) handleRequest(request *workerRequest, sock *zmq.Socket) {

	rqproto := new(proto.RPCRequest)
	pberr := pb.Unmarshal(request.data, rqproto)

	if pberr != nil {
		log.CRPC_log(log.LOGLEVEL_ERRORS, fmt.Sprintf("[%x/_/_] PB unmarshaling error: %s", request.client_id, pberr.Error()))

		// We can't send an error response because we don't even have a sequence number
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_SERVER_ERROR, request)
		return
	}

	caller_id := rqproto.GetCallerId()

	// It is already too late... we can discard this request
	if rqproto.GetDeadline() > 0 && time.Now().Unix() > rqproto.GetDeadline() {
		delta := time.Now().Unix() - rqproto.GetDeadline()

		log.CRPC_log(log.LOGLEVEL_WARNINGS, fmt.Sprintf("[%x/%s/%d] Timeout occurred, deadline was %d (%d s)",
			request.client_id, caller_id, rqproto.GetSequenceNumber(), rqproto.GetDeadline(), delta))

		// Sending this to get the REQ socket in the right state
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_MISSED_DEADLINE, request)
		return
	}

	// Find matching endpoint
	srvc, srvc_found := srv.services[rqproto.GetSrvc()]

	if !srvc_found {
		log.CRPC_log(log.LOGLEVEL_WARNINGS, fmt.Sprintf("[%x/%s/%d] NOT_FOUND response to request for service %s", request.client_id, caller_id, rqproto.GetSequenceNumber(), rqproto.GetSrvc()))

		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_NOT_FOUND, request)
		return
	} else {
		if handler, endpoint_found := srvc.endpoints[rqproto.GetProcedure()]; !endpoint_found {
			log.CRPC_log(log.LOGLEVEL_WARNINGS,
				fmt.Sprintf("[%x/%s/%d] NOT_FOUND response to request for endpoint %s",
					request.client_id, caller_id, rqproto.GetSequenceNumber(), rqproto.GetSrvc()+"."+rqproto.GetProcedure()))

			srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_NOT_FOUND, request)
			return
		} else {
			log.CRPC_log(log.LOGLEVEL_DEBUG,
				fmt.Sprintf("[%x/%s/%d] Calling endpoint %s.%s...",
					request.client_id, caller_id, rqproto.GetSequenceNumber(), rqproto.GetSrvc(), rqproto.GetProcedure()))

			cx := srv.newContext(rqproto, srv.rpclogger)

			// Actual invocation!!
			handler(cx)

			rpproto := cx.toRPCResponse()
			rpproto.SequenceNumber = pb.Uint64(rqproto.GetSequenceNumber())

			response_serialized, pberr := rpproto.Marshal()

			if pberr != nil {
				srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_SERVER_ERROR, request)

				log.CRPC_log(log.LOGLEVEL_ERRORS,
					fmt.Sprintf("[%x/%s/%d] Error when serializing RPCResponse: %s",
						request.client_id, caller_id, rqproto.GetSequenceNumber(), pberr.Error()))

			} else {

				_, err := sock.SendMessage(request.client_id, request.request_id, "", response_serialized)

				if err != nil {
					log.CRPC_log(log.LOGLEVEL_WARNINGS,
						fmt.Sprintf("[%x/%s/%d] Error when sending response; %s",
							request.client_id, caller_id, rqproto.GetSequenceNumber(), err.Error()))

					return
				}

				log.CRPC_log(log.LOGLEVEL_DEBUG, fmt.Sprintf("[%x/%s/%d] Sent response.", request.client_id, caller_id, rqproto.GetSequenceNumber()))

			}
		}
	}
}

// "one-shot" -- doesn't catch Write() errors. But needs a lot of context
func (srv *Server) sendError(sock *zmq.Socket, rq *proto.RPCRequest, s proto.RPCResponse_Status, request *workerRequest) {
	// The context functions do most of the work for us.
	tmp_ctx := srv.newContext(rq, nil)
	tmp_ctx.Fail(s.String())

	response := tmp_ctx.toRPCResponse()
	response.SequenceNumber = rq.SequenceNumber
	response.ResponseStatus = s.Enum()

	buf, err := pb.Marshal(response)

	if err != nil {
		return // Let the client time out. We can't do anything (although this isn't supposed to happen)
	}

	sock.SendMessage(request.client_id, request.request_id, "", buf)
}
