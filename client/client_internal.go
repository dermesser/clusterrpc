package client

import (
	"clusterrpc"
	"clusterrpc/proto"
	"clusterrpc/server"
	"errors"
	"fmt"
	"time"

	pb "github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
)

// (Re)connect to the peers that are stored in the Client object
func (cl *Client) createChannel() error {

	if cl.channel != nil {
		cl.channel.Close()
	}

	var err error
	cl.channel, err = zmq.NewSocket(zmq.REQ)

	if err != nil {
		cl.logger.Println("Error when creating Req socket:", err.Error())
		return &RequestError{status: proto.RPCResponse_STATUS_CLIENT_NETWORK_ERROR, err: err}
	}

	cl.channel.SetIpv6(true)
	cl.channel.SetReconnectIvl(100 * time.Millisecond)

	err = cl.connectToPeers()

	if err != nil {
		return err
	}

	cl.channel.SetSndtimeo(cl.timeout)
	cl.channel.SetRcvtimeo(cl.timeout)
	cl.channel.SetReqRelaxed(1)
	cl.channel.SetReqCorrelate(1)

	return nil
}

// Actual connecting takes place here
func (cl *Client) connectToPeers() error {

	for i := range cl.raddr {
		peer := fmt.Sprintf("tcp://%s:%d", cl.raddr[i], cl.rport[i])

		// If it was previously connected, first disconnect all
		cl.channel.Disconnect(peer)
		err := cl.channel.Connect(peer)

		if err != nil {
			if len(cl.raddr) < 2 { // only return error if the only connection of this REQ socket couldn't be established
				if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
					cl.logger.Println("Could not establish connection to single peer;",
						err.Error, fmt.Sprintf("tcp://%s:%d", cl.raddr[i], cl.rport[i]))
				}
				return &RequestError{status: proto.RPCResponse_STATUS_CLIENT_NETWORK_ERROR, err: err}
			} else {
				if cl.loglevel >= clusterrpc.LOGLEVEL_WARNINGS {
					cl.logger.Println("Error when connecting Req socket:",
						err.Error(), fmt.Sprintf("tcp://%s:%d", cl.raddr[i], cl.rport[i]))
				}
			}
		}
	}
	return nil
}

// Use only for redirected requests, otherwise the tracing doesn't work as intended!
func requestRedirect(raddr string, rport uint, service, endpoint string, request_data []byte,
	allow_redirect bool, settings_cl *Client, trace_dest *proto.TraceInfo) ([]byte, error) {
	var cl *Client
	var err error

	if settings_cl != nil {
		cl, err = NewClient(settings_cl.name+"_redir", raddr, rport, settings_cl.loglevel)

		if err != nil {
			return nil, &RequestError{status: proto.RPCResponse_STATUS_CLIENT_REQUEST_ERROR, err: err}
		}

		cl.loglevel = settings_cl.loglevel
		cl.logger = settings_cl.logger
		cl.SetTimeout(settings_cl.timeout)
		cl.accept_redirect = allow_redirect
	} else {
		cl, err = NewClient("anonymous_tmp_client", raddr, rport, clusterrpc.LOGLEVEL_WARNINGS)

		if err != nil {
			return nil, &RequestError{status: proto.RPCResponse_STATUS_CLIENT_REQUEST_ERROR, err: err}
		}
	}

	defer cl.Close()

	var new_trace_dest *proto.TraceInfo

	// If a trace is wanted, append this call to the current trace info (because this is a redirect)
	if trace_dest != nil {
		new_trace_dest = new(proto.TraceInfo)
		trace_dest.ChildCalls = append(trace_dest.ChildCalls, new_trace_dest)
	}

	rsp, err := cl.Request(request_data, service, endpoint, new_trace_dest)

	if err != nil {
		return rsp, err
	}

	return rsp, nil
	// Close() is deferred
}

/*
Request __CLUSTERRPC.Health and return true or false, respectively. If true is returned, the backend
was reachable and did return a positive answer within the timeout. Otherwise there should be no further
requests made.
*/
func (cl *Client) doHealthCheck(timeout time.Duration) bool {
	_, err := cl.Request([]byte{}, "__CLUSTERRPC", "Health", nil)

	if err == nil {
		return true
	} else {
		if cl.loglevel >= clusterrpc.LOGLEVEL_WARNINGS {
			cl.logger.Printf("RPC backend is unhealthy: %s\n", err.(*RequestError).Status())
		}
		return false
	}
}

/*
Prepare request and call Client.sendRequest() to send the request. Internally used only.
*/
func (cl *Client) request(cx *server.Context, trace_dest *proto.TraceInfo, data []byte,
	service, endpoint string) ([]byte, error) {

	cl.lock.Lock()
	defer cl.lock.Unlock()

	// Avoid recursion
	if cl.do_healthcheck && service != "__CLUSTERRPC" {
		cl.lock.Unlock()
		result := cl.doHealthCheck(1 * time.Second)
		cl.lock.Lock()
		if !result {
			return nil, &RequestError{status: proto.RPCResponse_STATUS_UNHEALTHY, err: errors.New("RPC backend unhealthy")}
		}
	}

	rqproto := proto.RPCRequest{}

	rqproto.SequenceNumber = pb.Uint64(cl.sequence_number)
	cl.sequence_number++

	rqproto.Srvc = pb.String(service)
	rqproto.Procedure = pb.String(endpoint)
	rqproto.Data = data
	rqproto.CallerId = pb.String(cl.name)

	// Deadline from context has precedence.
	if cx != nil {
		rqproto.Deadline = pb.Int64(cx.GetDeadline().UnixNano() / 1000)
	} else if cl.deadline_propagation && cl.timeout > 0 {
		rqproto.Deadline = pb.Int64((time.Now().UnixNano() + cl.timeout.Nanoseconds()) / 1000)
	}

	if trace_dest != nil || (cx != nil && cx.GetTraceInfo() != nil) {
		rqproto.WantTrace = pb.Bool(true)
	}

	msg, err := cl.sendRequest(&rqproto)

	if err != nil {
		return nil, err
	}

	cl.last_used = time.Now()

	respproto := proto.RPCResponse{}

	err = pb.Unmarshal(msg, &respproto)

	if err != nil {
		if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
			cl.logger.Printf("[%s/%d] Error when unmarshaling response: %s\n", cl.name, rqproto.GetSequenceNumber(), err.Error())
		}
		return nil, &RequestError{status: proto.RPCResponse_STATUS_CLIENT_REQUEST_ERROR, err: err}
	}

	if trace_dest != nil && respproto.GetTraceinfo() != nil {
		// Store the received traceinfo in trace_dest and/or context (usually one of both)
		*trace_dest = *respproto.GetTraceinfo()
	}
	if cx != nil {
		cx.AppendCallTrace(respproto.GetTraceinfo())
	}

	if respproto.GetResponseStatus() != proto.RPCResponse_STATUS_OK && respproto.GetResponseStatus() != proto.RPCResponse_STATUS_REDIRECT {
		if cl.loglevel >= clusterrpc.LOGLEVEL_WARNINGS {
			cl.logger.Printf("[%s/%d] Received status other than ok from %s: %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint, respproto.GetResponseStatus().String())
		}
		return nil, &RequestError{status: respproto.GetResponseStatus(), err: nil}
	} else if respproto.GetResponseStatus() == proto.RPCResponse_STATUS_REDIRECT {
		if cl.accept_redirect {
			if respproto.GetRedirService() == "" || respproto.GetRedirEndpoint() == "" { // No different service.endpoint given, retry with same method
				return requestRedirect(respproto.GetRedirHost(), uint(respproto.GetRedirPort()),
					service, endpoint, data, false, cl, trace_dest)
			} else {
				return requestRedirect(respproto.GetRedirHost(), uint(respproto.GetRedirPort()),
					respproto.GetRedirService(), respproto.GetRedirEndpoint(), data, false, cl, trace_dest)
			}
		} else {
			if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
				cl.logger.Printf("[%s/%d] Could not follow redirect -- second server redirected, too\n", cl.name, rqproto.GetSequenceNumber())
			}
			return nil, &RequestError{status: proto.RPCResponse_STATUS_REDIRECT_TOO_OFTEN, err: nil}
		}
	}

	return respproto.GetResponseData(), nil
}

/*
Actually send and receive.
*/
func (cl *Client) sendRequest(rqproto *proto.RPCRequest) ([]byte, error) {
	// cl is already locked

	rq_serialized, pberr := pb.Marshal(rqproto)

	if pberr != nil {
		if cl.loglevel >= clusterrpc.LOGLEVEL_WARNINGS {
			cl.logger.Println("PB error!", pberr.Error())
		}
		return nil, &RequestError{status: proto.RPCResponse_STATUS_CLIENT_REQUEST_ERROR, err: pberr}
	}

	for i := cl.eagain_retries; i >= 0; i-- {
		_, err := cl.channel.SendBytes(rq_serialized, 0)

		if err != nil {
			if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
				cl.logger.Printf("[%s/%d] Could not send message to %s. Error: %s\n", cl.name, rqproto.GetSequenceNumber(), rqproto.GetSrvc()+"."+rqproto.GetProcedure(), err.Error())
			}
			if err.(zmq.Errno) == 11 { // EAGAIN

				if len(cl.raddr) < 2 {
					cl.createChannel()
				}

				if i > 0 {
					continue
				}

				return nil, &RequestError{status: proto.RPCResponse_STATUS_TIMEOUT, err: err}
			} else {
				if len(cl.raddr) < 2 {
					cl.createChannel()
				}

				if i > 0 {
					continue
				}
				return nil, &RequestError{status: proto.RPCResponse_STATUS_CLIENT_NETWORK_ERROR, err: err}
			}
		} else {
			if cl.loglevel >= clusterrpc.LOGLEVEL_DEBUG {
				cl.logger.Printf("[%s/%d] Sent request to %s\n", cl.name, rqproto.GetSequenceNumber(), rqproto.GetSrvc()+"."+rqproto.GetProcedure())
			}
			break
		}
	}

	msg, err := cl.channel.RecvBytes(0)

	if err != nil {
		if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
			cl.logger.Printf("[%s/%d] Could not receive response from %s, error %s\n", cl.name, rqproto.GetSequenceNumber(), rqproto.GetSrvc()+"."+rqproto.GetProcedure(), err.Error())
		}
		if 11 == err.(zmq.Errno) { // We have no retries left.
			// Reconnect if there is only one peer (otherwise send() times out b/c the peer connection
			// is removed from the set in the REQ socket)
			if len(cl.raddr) < 2 {
				cl.createChannel()
			}
			if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
				cl.logger.Printf("[%s/%d] Timeout occurred, retries failed. Giving up\n", cl.name, rqproto.GetSequenceNumber())
			}
			return nil, &RequestError{status: proto.RPCResponse_STATUS_TIMEOUT, err: err}
		} else {
			if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
				cl.logger.Printf("[%s/%d] Network error: %s\n", cl.name, rqproto.GetSequenceNumber(), err.Error())
			}
			return nil, &RequestError{status: proto.RPCResponse_STATUS_CLIENT_NETWORK_ERROR, err: err}
		}
	}
	if cl.loglevel >= clusterrpc.LOGLEVEL_DEBUG {
		cl.logger.Printf("[%s/%d] Received response from %s\n", cl.name, rqproto.GetSequenceNumber(), rqproto.GetSrvc()+"."+rqproto.GetProcedure())
	}

	return msg, nil
}
