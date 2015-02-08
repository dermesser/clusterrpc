package clusterrpc

import (
	"clusterrpc/proto"
	"container/list"
	"fmt"
	"time"

	pb "code.google.com/p/goprotobuf/proto"
	zmq "github.com/pebbe/zmq4"
)

const BACKEND_ROUTER_PATH string = "inproc://rpc_backend_router"
const MAGIC_READY_STRING string = "___//READY\\___"
const OUTSTANDING_REQUESTS_PER_THREAD int = 50

/*
This file has the internal functions, the actual server; server.go remains
uncluttered and with only public functions.
*/

/*
Load balancer using the least used worker: We have a list (queue) of backend worker identities;
a backend is queued when it sends a response, and dequeued when it is sent a client request.

Additionally, there's a request queue for the case that there are no workers available at the moment.
This queue is consulted every time a worker is done with a request, which results in a relatively
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
					msgs, err := srv.frontend_router.RecvMessageBytes(0) // [client identity, "", RPCRequest]

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
						_, err = srv.backend_router.SendMessage(worker_id.Value.([]byte), "", msgs) // [worker identity, "", client identity, "", RPCRequest]
						worker_queue.Remove(worker_id)

						if err != nil && srv.loglevel >= LOGLEVEL_ERRORS {
							srv.logger.Println("Error when sending to backend router:", err.Error())
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
						if srv.loglevel >= LOGLEVEL_WARNINGS { // Could not queue, drop
							srv.logger.Println("Dropped message; no available workers, queue full")
						}
						// Maybe just drop silently -- this costs CPU!
						request := proto.RPCRequest{}
						err = pb.Unmarshal(msgs[2], &request)

						if err != nil {
							continue
						}

						response := proto.RPCResponse{}
						response.ResponseStatus = new(proto.RPCResponse_Status)
						*response.ResponseStatus = proto.RPCResponse_STATUS_OVERLOADED_RETRY
						response.ErrorMessage = pb.String("Could not accept request because our queue is full. Retry later")
						response.SequenceNumber = pb.Uint64(request.GetSequenceNumber())

						respproto, err := pb.Marshal(&response)

						if err != nil {
							continue
						}

						srv.frontend_router.SendMessage(msgs[0:2], respproto)
					}

				case srv.backend_router:
					msgs, err := srv.backend_router.RecvMessageBytes(0) // [worker identity, "", client identity, "", RPCResponse]

					if err != nil {
						if srv.loglevel >= LOGLEVEL_ERRORS {
							srv.logger.Println("Error when receiving from frontend:", err.Error())
							continue
						}
					}
					if len(msgs) < 3 {
						if srv.loglevel >= LOGLEVEL_ERRORS {
							srv.logger.Println("Error: Skipping message with less than 3 frames from backend;", len(msgs), "frames received")
						}
						continue
					}

					backend_identity := msgs[0]
					worker_queue.PushBack(backend_identity)

					// third frame is MAGIC_READY_STRING when a new worker joins.
					if string(msgs[2]) != MAGIC_READY_STRING { // if not MAGIC_READY_STRING, it's a client identity.
						_, err := srv.frontend_router.SendMessage(msgs[2:]) // [client identity, "", RPCResponse]

						if err != nil {
							if srv.loglevel >= LOGLEVEL_ERRORS {
								srv.logger.Println("Error when sending to backend router:", err.Error())
							}
						}
					}

					// Now that we have a new free worker, let's see if there's work in the queue...
					if todo_queue.Len() > 0 && worker_queue.Len() > 0 {
						request_message := todo_queue.Remove(todo_queue.Front()).([][]byte)
						worker_id := worker_queue.Remove(worker_queue.Front()).([]byte)
						_, err = srv.backend_router.SendMessage(worker_id, "", request_message) // [worker identity, "", client identity, "", RPCRequest]
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

// Start a single thread; spawn a goroutine if spawn == true. Otherwise, execute in the current thread
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

	sock.SendMessage(MAGIC_READY_STRING)
	for {
		msgs, err := sock.RecvMessageBytes(0)

		if err == nil && len(msgs) >= 3 {
			if srv.loglevel >= LOGLEVEL_DEBUG {
				srv.logger.Printf("Worker #%s received message from %x\n", worker_identity, msgs[0])
			}
			srv.handleRequest(msgs[2], msgs[0], sock)
		} else {
			if srv.loglevel >= LOGLEVEL_WARNINGS {
				if err != nil {
					srv.logger.Println("Skipped incoming message, error:", err.Error())
				} else if len(msgs) < 3 {
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
func (srv *Server) handleRequest(data, client_identity []byte, sock *zmq.Socket) {

	rqproto := proto.RPCRequest{}
	pberr := pb.Unmarshal(data, &rqproto)

	if pberr != nil {
		if srv.loglevel >= LOGLEVEL_ERRORS {
			srv.logger.Printf("[%x/_/_] PB unmarshaling error: %s", client_identity, pberr.Error())
		}
		// We can't send an error response because we don't even have a sequence number
		sock.SendMessage("")
		return
	}

	caller_id := rqproto.GetCallerId()

	// It is too late... we can discard this request
	if rqproto.GetDeadline() > 0 && time.Now().Unix() > rqproto.GetDeadline() {
		if srv.loglevel >= LOGLEVEL_WARNINGS {
			delta := time.Now().Unix() - rqproto.GetDeadline()
			srv.logger.Printf("[%x/%s/%d] Timeout occurred, deadline was %d (%d s)", client_identity, caller_id, rqproto.GetSequenceNumber(), rqproto.GetDeadline(), delta)
		}
		// Sending this to get the REQ socket in the right state
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_TIMEOUT, client_identity)
		return
	}

	// Find matching endpoint
	srvc, srvc_found := srv.services[rqproto.GetSrvc()]

	if !srvc_found {
		if srv.loglevel >= LOGLEVEL_WARNINGS {
			srv.logger.Printf("[%x/%s/%d] NOT_FOUND response to request for service %s\n", client_identity, caller_id, rqproto.GetSequenceNumber(), rqproto.GetSrvc())
		}
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_NOT_FOUND, client_identity)
		return
	} else {
		if handler, endpoint_found := srvc.endpoints[rqproto.GetProcedure()]; !endpoint_found {
			if srv.loglevel >= LOGLEVEL_WARNINGS {
				srv.logger.Printf("[%x/%s/%d] NOT_FOUND response to request for endpoint %s\n", client_identity, caller_id, rqproto.GetSequenceNumber(), rqproto.GetSrvc()+"."+rqproto.GetProcedure())
			}
			srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_NOT_FOUND, client_identity)
			return
		} else {
			if srv.loglevel >= LOGLEVEL_DEBUG {
				srv.logger.Printf("[%x/%s/%d] Calling endpoint %s.%s...\n", client_identity, caller_id, rqproto.GetSequenceNumber(), rqproto.GetSrvc(), rqproto.GetProcedure())
			}
			cx := newContext([]byte(rqproto.GetData()))
			handler(cx)

			rpproto := cx.toRPCResponse()
			rpproto.SequenceNumber = pb.Uint64(rqproto.GetSequenceNumber())

			response_serialized, pberr := pb.Marshal(&rpproto)

			if pberr != nil {
				srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_SERVER_ERROR, client_identity)

				if srv.loglevel >= LOGLEVEL_ERRORS {
					srv.logger.Printf("[%x/%s/%d] Error when serializing RPCResponse: %s\n", client_identity, caller_id, rqproto.GetSequenceNumber(), pberr.Error())
				}
			} else {

				_, err := sock.SendMessage(client_identity, "", response_serialized)

				if err != nil {
					if srv.loglevel >= LOGLEVEL_WARNINGS {
						srv.logger.Printf("[%x/%s/%d] Error when sending response; %s\n", client_identity, caller_id, rqproto.GetSequenceNumber(), err.Error())
					}
					return
				}

				if srv.loglevel >= LOGLEVEL_DEBUG {
					srv.logger.Printf("[%x/%s/%d] Sent response.\n", client_identity, caller_id, rqproto.GetSequenceNumber())
				}
			}
		}
	}
}

// "one-shot" -- doesn't catch Write() errors. But needs a lot of context
func (srv *Server) sendError(sock *zmq.Socket, rq proto.RPCRequest, s proto.RPCResponse_Status, client_id []byte) {
	response := proto.RPCResponse{}
	response.SequenceNumber = rq.SequenceNumber

	response.ResponseStatus = new(proto.RPCResponse_Status)
	*response.ResponseStatus = s

	buf, err := pb.Marshal(&response)

	if err != nil {
		return // Let the client time out. We can't do anything (although this isn't supposed to happen)
	}

	sock.SendMessage(client_id, "", buf)
}
