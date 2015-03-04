package clusterrpc

import (
	"bytes"
	"clusterrpc/proto"
	"container/list"
	"fmt"
	"time"

	pb "github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
)

const BACKEND_ROUTER_PATH string = "inproc://rpc_backend_router"

var MAGIC_READY_STRING []byte = []byte("___ReAdY___")

const OUTSTANDING_REQUESTS_PER_THREAD int = 50

type workerRequest struct {
	request_id, client_id, data []byte
}

/*
This file has the internal functions, the actual server; server.go remains
uncluttered and with only public functions.
*/

/*
Load balancer using the least used worker: We have a list (queue) of backend worker identities;
a backend is queued when it sends a response, and dequeued when it is sent a client request.

Additionally, there's a request queue for the case that there are no workers available at the moment.
This queue is consulted every time a worker has completed a request, which results in a relatively
good resource efficiency.
*/
func (srv *Server) loadbalance() {
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
			if srv.loglevel >= LOGLEVEL_ERRORS {
				srv.logger.Println("Polling error in loadbalancer:", err.Error())
			}
			continue
		} else {
			for _, sock := range polled {
				switch s := sock.Socket; s {
				case srv.frontend_router:
					// The message we're receiving here has this format: [client_identity, request_id, "", data].
					// request_id is a kind of sequence number and is sent because we set ZMQ_REQ_CORRELATE. client_identity
					// is added by the router.
					msgs, err := srv.frontend_router.RecvMessageBytes(0)

					if err != nil {
						if srv.loglevel >= LOGLEVEL_ERRORS {
							srv.logger.Println("Error when receiving from frontend:", err.Error())
							continue
						}
					}
					if len(msgs) < 3 {
						if srv.loglevel >= LOGLEVEL_ERRORS {
							srv.logger.Println(
								"Error: Skipping message with less than 3 frames from frontend router;", len(msgs), "frames received")
						}
						continue
					}

					// Try to find worker to send this request to
					if worker_id := worker_queue.Front(); worker_id != nil {
						_, err = srv.backend_router.SendMessage(worker_id.Value.([]byte), "", msgs) // [worker identity, "", client identity, request_id, "", RPCRequest]
						worker_queue.Remove(worker_id)

						if err != nil && srv.loglevel >= LOGLEVEL_ERRORS {
							if err.(zmq.Errno) != zmq.EHOSTUNREACH {
								srv.logger.Println("Error when sending to backend router:", err.Error())
							} else {
								srv.logger.Printf("Could not route message, identity %s, to frontend\n", msgs[0])
							}
						}
					} else if todo_queue.Len() < srv.n_threads*OUTSTANDING_REQUESTS_PER_THREAD { // We're only allowing so many queued requests to prevent from complete overloading
						todo_queue.PushBack(msgs)

						if srv.loglevel >= LOGLEVEL_WARNINGS &&
							todo_queue.Len() > int(0.8*float64(srv.n_threads*OUTSTANDING_REQUESTS_PER_THREAD)) {

							srv.logger.Printf("Queue is now at more than 80% fullness. Consider increasing # of workers (%d/%d)",
								todo_queue.Len(), srv.n_threads*OUTSTANDING_REQUESTS_PER_THREAD)

						} else if srv.loglevel >= LOGLEVEL_DEBUG {
							srv.logger.Println("Queued request. Current queue length:", todo_queue.Len())
						}
					} else {
						// Maybe just drop silently -- this costs CPU!
						request := proto.RPCRequest{}
						err = pb.Unmarshal(msgs[2], &request)

						if err != nil {
							if srv.loglevel >= LOGLEVEL_WARNINGS { // Could not queue, drop
								srv.logger.Println("Dropped message; no available workers, queue full")
							}
							continue
						}

						response := proto.RPCResponse{}
						response.ResponseStatus = new(proto.RPCResponse_Status)
						*response.ResponseStatus = proto.RPCResponse_STATUS_OVERLOADED_RETRY
						response.ErrorMessage = pb.String("Could not accept request because our queue is full. Retry later")
						response.SequenceNumber = pb.Uint64(request.GetSequenceNumber())

						respproto, err := pb.Marshal(&response)

						if err != nil {
							if srv.loglevel >= LOGLEVEL_WARNINGS { // Could not queue, drop
								srv.logger.Println("Dropped message; no available workers, queue full")
							}
							continue
						}

						_, err = srv.frontend_router.SendMessage(msgs[0:2], respproto)

						if err != nil {
							if srv.loglevel >= LOGLEVEL_ERRORS {
								srv.logger.Printf("Could not route message, identity %s, to frontend\n", msgs[0])
							}
						}
					}

				case srv.backend_router:
					msgs, err := srv.backend_router.RecvMessageBytes(0) // 6 frames: [worker identity, "", client identity, request_id, "", RPCResponse]

					if err != nil {
						if srv.loglevel >= LOGLEVEL_ERRORS {
							srv.logger.Println("Error when receiving from frontend:", err.Error())
							continue
						}
					}
					if len(msgs) < 6 { // We're expecting 6 frames, no less.
						if srv.loglevel >= LOGLEVEL_ERRORS {
							srv.logger.Println("Error: Skipping message with less than 6 frames from backend;", len(msgs), "frames received")
						}
						continue
					}

					backend_identity := msgs[0]
					worker_queue.PushBack(backend_identity)

					// third frame is MAGIC_READY_STRING when a new worker joins.
					if bytes.Equal(msgs[5], MAGIC_READY_STRING) { // if not MAGIC_READY_STRING, it's an RPCResponse.
						_, err := srv.frontend_router.SendMessage(msgs[2:]) // [client identity, request_id, "", RPCResponse]

						if err != nil && srv.loglevel >= LOGLEVEL_WARNINGS {
							if err.(zmq.Errno) != zmq.EHOSTUNREACH {
								srv.logger.Println("Error when sending to backend router:", err.Error())
							} else if err.(zmq.Errno) == zmq.EHOSTUNREACH { // routing is mandatory
								srv.logger.Printf("Could not route message, identity %s, to frontend\n", msgs[0])
							}
						}
					}

					// Now that we have a new free worker, let's see if there's work in the queue...
					if todo_queue.Len() > 0 && worker_queue.Len() > 0 {
						request_message := todo_queue.Remove(todo_queue.Front()).([][]byte)
						worker_id := worker_queue.Remove(worker_queue.Front()).([]byte)
						_, err = srv.backend_router.SendMessage(worker_id, "", request_message) // [worker identity, "", client identity, request_id, "", RPCRequest]
						if err != nil {
							if srv.loglevel >= LOGLEVEL_ERRORS {
								srv.logger.Println("Error when sending to backend router:", err.Error())
							}
						}
					}
				}
			}
		}
	}
}

// Start a single worker thread; spawn a goroutine if spawn == true. Otherwise, execute in the current thread.
// This thread will later execute the registered handlers.
func (srv *Server) thread(n int, spawn bool) error {
	// Yes, we're using a REQ socket for the worker
	// see http://zguide.zeromq.org/page:all#toc72
	sock, err := srv.zmq_context.NewSocket(zmq.REQ)

	if err != nil {
		if srv.loglevel >= LOGLEVEL_ERRORS {
			srv.logger.Println("Thread", n, "could not create socket, exiting!")
		}
		return err
	}

	worker_identity := fmt.Sprintf("%d", n)
	err = sock.SetIdentity(worker_identity)

	if err != nil {
		if srv.loglevel >= LOGLEVEL_ERRORS {
			srv.logger.Println("Thread", n, "could not set identity, exiting!")
		}
		return err
	}

	err = sock.Connect(BACKEND_ROUTER_PATH)

	if err != nil {
		srv.logger.Println("Thread", n, "could not connect to Dealer, exiting!")
		return err
	}

	sock.SetSndtimeo(srv.timeout)

	if !spawn {
		go srv.acceptRequests(sock, worker_identity)
	} else {
		srv.acceptRequests(sock, worker_identity)
	}
	return nil
}

// This function runs in the (few) threads of the RPC server.
func (srv *Server) acceptRequests(sock *zmq.Socket, worker_identity string) error {

	// Send bogus parts so we have the correct number of frames in this message. (doesn't impact performance)
	sock.SendMessage("___BOGUS_CLIENT_ID", "___BOGUS_REQUEST_ID", "", MAGIC_READY_STRING)
	for {
		// We're getting here the following message parts: [client_identity, request_id, "", data]
		msgs, err := sock.RecvMessageBytes(0)

		if err == nil && len(msgs) >= 4 {
			if srv.loglevel >= LOGLEVEL_DEBUG {
				srv.logger.Printf("Worker #%s received message from %x\n", worker_identity, msgs[0])
			}
			req := workerRequest{client_id: msgs[0], request_id: msgs[1], data: msgs[3]}
			srv.handleRequest(&req, sock)
		} else {
			if srv.loglevel >= LOGLEVEL_WARNINGS {
				if err != nil {
					srv.logger.Println("Skipped incoming message, error:", err.Error())
				} else if len(msgs) < 4 {
					srv.logger.Println("Frontend router gave message with less than 3 frames")
				}
			}
			continue
		}

	}
	return nil
}

// Handle one request.
// client_identity is the unique number assigned by ZeroMQ. data is the raw data input from the client.
func (srv *Server) handleRequest(request *workerRequest, sock *zmq.Socket) {

	rqproto := proto.RPCRequest{}
	pberr := pb.Unmarshal(request.data, &rqproto)

	if pberr != nil {
		if srv.loglevel >= LOGLEVEL_ERRORS {
			srv.logger.Printf("[%x/_/_] PB unmarshaling error: %s", request.client_id, pberr.Error())
		}
		// We can't send an error response because we don't even have a sequence number
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_SERVER_ERROR, request)
		return
	}

	caller_id := rqproto.GetCallerId()

	// It is too late... we can discard this request
	if rqproto.GetDeadline() > 0 && time.Now().Unix() > rqproto.GetDeadline() {
		if srv.loglevel >= LOGLEVEL_WARNINGS {
			delta := time.Now().Unix() - rqproto.GetDeadline()
			srv.logger.Printf("[%x/%s/%d] Timeout occurred, deadline was %d (%d s)", request.client_id, caller_id, rqproto.GetSequenceNumber(), rqproto.GetDeadline(), delta)
		}
		// Sending this to get the REQ socket in the right state
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_TIMEOUT, request)
		return
	}

	// Find matching endpoint
	srvc, srvc_found := srv.services[rqproto.GetSrvc()]

	if !srvc_found {
		if srv.loglevel >= LOGLEVEL_WARNINGS {
			srv.logger.Printf("[%x/%s/%d] NOT_FOUND response to request for service %s\n", request.client_id, caller_id, rqproto.GetSequenceNumber(), rqproto.GetSrvc())
		}
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_NOT_FOUND, request)
		return
	} else {
		if handler, endpoint_found := srvc.endpoints[rqproto.GetProcedure()]; !endpoint_found {
			if srv.loglevel >= LOGLEVEL_WARNINGS {
				srv.logger.Printf("[%x/%s/%d] NOT_FOUND response to request for endpoint %s\n", request.client_id, caller_id, rqproto.GetSequenceNumber(), rqproto.GetSrvc()+"."+rqproto.GetProcedure())
			}
			srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_NOT_FOUND, request)
			return
		} else {
			if srv.loglevel >= LOGLEVEL_DEBUG {
				srv.logger.Printf("[%x/%s/%d] Calling endpoint %s.%s...\n", request.client_id, caller_id, rqproto.GetSequenceNumber(), rqproto.GetSrvc(), rqproto.GetProcedure())
			}
			cx := newContext(rqproto.GetData())
			handler(cx)

			rpproto := cx.toRPCResponse()
			rpproto.SequenceNumber = pb.Uint64(rqproto.GetSequenceNumber())

			response_serialized, pberr := pb.Marshal(&rpproto)

			if pberr != nil {
				srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_SERVER_ERROR, request)

				if srv.loglevel >= LOGLEVEL_ERRORS {
					srv.logger.Printf("[%x/%s/%d] Error when serializing RPCResponse: %s\n", request.client_id, caller_id, rqproto.GetSequenceNumber(), pberr.Error())
				}
			} else {

				_, err := sock.SendMessage(request.client_id, request.request_id, "", response_serialized)

				if err != nil {
					if srv.loglevel >= LOGLEVEL_WARNINGS {
						srv.logger.Printf("[%x/%s/%d] Error when sending response; %s\n", request.client_id, caller_id, rqproto.GetSequenceNumber(), err.Error())
					}
					return
				}

				if srv.loglevel >= LOGLEVEL_DEBUG {
					srv.logger.Printf("[%x/%s/%d] Sent response.\n", request.client_id, caller_id, rqproto.GetSequenceNumber())
				}
			}
		}
	}
}

// "one-shot" -- doesn't catch Write() errors. But needs a lot of context
func (srv *Server) sendError(sock *zmq.Socket, rq proto.RPCRequest, s proto.RPCResponse_Status, request *workerRequest) {
	response := proto.RPCResponse{}

	response.SequenceNumber = pb.Uint64(rq.GetSequenceNumber()) // may be zero
	response.ResponseStatus = new(proto.RPCResponse_Status)
	*response.ResponseStatus = s

	buf, err := pb.Marshal(&response)

	if err != nil {
		return // Let the client time out. We can't do anything (although this isn't supposed to happen)
	}

	sock.SendMessage(request.client_id, request.request_id, "", buf)
}
