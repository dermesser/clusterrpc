package clusterrpc

import (
	"clusterrpc/proto"
	"fmt"
	"time"

	pb "code.google.com/p/goprotobuf/proto"
	zmq "github.com/pebbe/zmq4"
)

/*
This file has the internal functions, the actual server; server.go remains
uncluttered and with only public functions.
*/

/*
Load balancer using the least used worker: We have a channel of backend identities.
A backend is queued when it sends a response, and dequeued when we have a client request.
*/
func (srv *Server) loadbalance() {
	queue := make(chan string, srv.n_threads)

	poller := zmq.NewPoller()
	poller.Add(srv.frontend_router, zmq.POLLIN)
	poller.Add(srv.backend_router, zmq.POLLIN)

	for true {
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
					msgs, err := srv.frontend_router.RecvMessage(0)

					if err != nil {
						if srv.loglevel >= LOGLEVEL_ERRORS {
							srv.logger.Println("Error when receiving from frontend:", err.Error())
							continue
						}
					}

					node := <-queue
					_, err = srv.backend_router.SendMessage(node, "", msgs)

					if err != nil {
						if srv.loglevel >= LOGLEVEL_ERRORS {
							srv.logger.Println("Error when sending to backend router:", err.Error())
						}
						continue
					}

				case srv.backend_router:
					msgs, err := srv.backend_router.RecvMessage(0)

					if err != nil {
						if srv.loglevel >= LOGLEVEL_ERRORS {
							srv.logger.Println("Error when receiving from frontend:", err.Error())
							continue
						}
					}

					backend_identity := msgs[0]
					queue <- backend_identity

					if msgs[2] != MAGIC_READY_STRING {
						_, err = srv.frontend_router.SendMessage(msgs[2:])
						if err != nil {
							if srv.loglevel >= LOGLEVEL_ERRORS {
								srv.logger.Println("Error when sending to backend router:", err.Error())
							}
						}
					} else {
						continue
					}
				}
			}
		}
	}
}

// Start a single thread; use "go" if spawn == true. Otherwise, execute in this routine.
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

	err = sock.SetIdentity(fmt.Sprintf("%d", n))

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
		go srv.acceptRequests(sock)
	} else {
		srv.acceptRequests(sock)
	}
	return nil
}

// This function runs in the (few) threads of the RPC server.
func (srv *Server) acceptRequests(sock *zmq.Socket) error {

	sock.SendMessage(MAGIC_READY_STRING)
	for true {
		msgs, err := sock.RecvMessageBytes(0)

		if srv.loglevel >= LOGLEVEL_INFO {
			srv.logger.Printf("Received message from %x\n", msgs[0])
		}
		if err == nil && len(msgs) >= 3 {
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
// TODO Maybe break this up a little bit?
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
	if rqproto.GetDeadline() > 0 && rqproto.GetDeadline() < time.Now().Unix() {
		if srv.loglevel >= LOGLEVEL_WARNINGS {
			delta := time.Now().Unix() - rqproto.GetDeadline()
			srv.logger.Printf("[%x/%s/%d] Timeout occurred, deadline was %d (%d s)", client_identity, caller_id, rqproto.GetSequenceNumber(), rqproto.GetDeadline(), delta)
		}
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_TIMEOUT)
		return
	}

	// Find matching endpoint
	srvc, srvc_found := srv.services[rqproto.GetSrvc()]

	if !srvc_found {
		if srv.loglevel >= LOGLEVEL_WARNINGS {
			srv.logger.Printf("[%x/%s/%d] NOT_FOUND response to request for service %s\n", client_identity, caller_id, rqproto.GetSequenceNumber(), rqproto.GetSrvc())
		}
		srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_NOT_FOUND)
		return
	} else {
		if handler, endpoint_found := srvc.endpoints[rqproto.GetProcedure()]; !endpoint_found {
			if srv.loglevel >= LOGLEVEL_WARNINGS {
				srv.logger.Printf("[%x/%s/%d] NOT_FOUND response to request for endpoint %s\n", client_identity, caller_id, rqproto.GetSequenceNumber(), rqproto.GetSrvc()+"."+rqproto.GetProcedure())
			}
			srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_NOT_FOUND)
			return
		} else {
			cx := NewContext([]byte(rqproto.GetData()))
			handler(cx)

			rpproto := srv.contextToRPCResponse(cx)
			rpproto.SequenceNumber = pb.Uint64(rqproto.GetSequenceNumber())

			response_serialized, pberr := pb.Marshal(&rpproto)

			if pberr != nil {
				srv.sendError(sock, rqproto, proto.RPCResponse_STATUS_SERVER_ERROR)

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

func (srv *Server) contextToRPCResponse(cx *Context) proto.RPCResponse {
	rpproto := proto.RPCResponse{}
	rpproto.ResponseStatus = new(proto.RPCResponse_Status)

	if !cx.failed {
		*rpproto.ResponseStatus = proto.RPCResponse_STATUS_OK
	} else {
		*rpproto.ResponseStatus = proto.RPCResponse_STATUS_NOT_OK
		rpproto.ErrorMessage = pb.String(cx.errorMessage)
	}

	rpproto.ResponseData = pb.String(string(cx.result))

	if cx.redirected {
		rpproto.RedirHost = pb.String(cx.redir_host)
		rpproto.RedirPort = pb.Uint32(cx.redir_port)
		*rpproto.ResponseStatus = proto.RPCResponse_STATUS_REDIRECT
	}

	return rpproto
}

// "one-shot" -- doesn't catch Write() errors
func (srv *Server) sendError(sock *zmq.Socket, rq proto.RPCRequest, s proto.RPCResponse_Status) {
	response := proto.RPCResponse{}
	response.SequenceNumber = rq.SequenceNumber

	response.ResponseStatus = new(proto.RPCResponse_Status)
	*response.ResponseStatus = s

	buf, err := pb.Marshal(&response)

	if err != nil {
		return // Let the client time out. We can't do anything (although this isn't supposed to happen)
	}

	// Will fail if we send a STATUS_TIMEOUT message because the client is likely to have closed the
	// connection by now.
	sock.SendMessage(buf)
}
