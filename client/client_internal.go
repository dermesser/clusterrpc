package client

import (
	"clusterrpc"
	"clusterrpc/proto"
	"clusterrpc/server"
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
		return RequestError{status: proto.RPCResponse_STATUS_CLIENT_NETWORK_ERROR, message: err.Error()}
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
				return RequestError{status: proto.RPCResponse_STATUS_CLIENT_NETWORK_ERROR, message: err.Error()}
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
		cl.loglevel = settings_cl.loglevel
		cl.logger = settings_cl.logger
		cl.SetTimeout(settings_cl.timeout)
		cl.accept_redirect = allow_redirect
	} else {
		cl, err = NewClient("anonymous_tmp_client", raddr, rport, clusterrpc.LOGLEVEL_WARNINGS)
	}

	if err != nil {
		return nil, RequestError{status: proto.RPCResponse_STATUS_CLIENT_REQUEST_ERROR, message: err.Error()}
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

func (cl *Client) request(cx *server.Context, trace_dest *proto.TraceInfo, data []byte,
	service, endpoint string, retries int) ([]byte, error) {

	cl.lock.Lock()
	defer cl.lock.Unlock()

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
	} else if cl.timeout > 0 {
		rqproto.Deadline = pb.Int64((time.Now().UnixNano() + cl.timeout.Nanoseconds()) / 1000)
	}

	if trace_dest != nil || (cx != nil && cx.GetTraceInfo() != nil) {
		rqproto.WantTrace = pb.Bool(true)
	}

	msg, err := cl.sendRequest(&rqproto, retries)

	if err != nil {
		return nil, err
	}

	respproto := proto.RPCResponse{}

	err = pb.Unmarshal(msg, &respproto)

	if err != nil {
		if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
			cl.logger.Printf("[%s/%d] Error when unmarshaling response: %s\n", cl.name, rqproto.GetSequenceNumber(), err.Error())
		}
		return nil, RequestError{status: proto.RPCResponse_STATUS_CLIENT_REQUEST_ERROR, message: err.Error()}
	}

	if trace_dest != nil {
		// Store the received traceinfo in trace_dest and/or context (usually one of both)
		if respproto.GetTraceinfo() != nil {
			*trace_dest = *respproto.GetTraceinfo()
		}
	}
	if cx != nil {
		cx.AppendCallTrace(respproto.GetTraceinfo())
	}

	if respproto.GetResponseStatus() != proto.RPCResponse_STATUS_OK && respproto.GetResponseStatus() != proto.RPCResponse_STATUS_REDIRECT {
		if cl.loglevel >= clusterrpc.LOGLEVEL_WARNINGS {
			cl.logger.Printf("[%s/%d] Received status other than ok from %s: %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint, statusToString(respproto.GetResponseStatus()))
		}
		return nil, RequestError{status: respproto.GetResponseStatus(), message: respproto.GetErrorMessage()}
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
			return nil, RequestError{status: proto.RPCResponse_STATUS_REDIRECT_TOO_OFTEN, message: "Could not follow redirects (redirect loop avoidance)"}
		}
	}

	return respproto.GetResponseData(), nil
}

func (cl *Client) sendRequest(rqproto *proto.RPCRequest, retries_left int) ([]byte, error) {
	// cl is already locked

	rq_serialized, pberr := pb.Marshal(rqproto)

	if pberr != nil {
		if cl.loglevel >= clusterrpc.LOGLEVEL_WARNINGS {
			cl.logger.Println("PB error!", pberr.Error())
		}
		return nil, RequestError{status: proto.RPCResponse_STATUS_CLIENT_REQUEST_ERROR, message: pberr.Error()}
	}

	_, err := cl.channel.SendBytes(rq_serialized, 0)

	if err != nil {
		if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
			cl.logger.Printf("[%s/%d] Could not send message to %s. Error: %s\n", cl.name, rqproto.GetSequenceNumber(), rqproto.GetSrvc()+"."+rqproto.GetProcedure(), err.Error())
		}
		if err.(zmq.Errno) == 11 { // EAGAIN
			cl.createChannel()
			return nil, RequestError{status: proto.RPCResponse_STATUS_TIMEOUT, message: err.Error()}
		} else {
			return nil, RequestError{status: proto.RPCResponse_STATUS_CLIENT_NETWORK_ERROR, message: err.Error()}
		}
	} else {
		if cl.loglevel >= clusterrpc.LOGLEVEL_DEBUG {
			cl.logger.Printf("[%s/%d] Sent request to %s\n", cl.name, rqproto.GetSequenceNumber(), rqproto.GetSrvc()+"."+rqproto.GetProcedure())
		}
	}

	msg, err := cl.channel.RecvBytes(0)

	if err != nil {
		if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
			cl.logger.Printf("[%s/%d] Could not receive response from %s, error %s\n", cl.name, rqproto.GetSequenceNumber(), rqproto.GetSrvc()+"."+rqproto.GetProcedure(), err.Error())
		}
		if 11 == uint32(err.(zmq.Errno)) && retries_left > 0 { // 11 == EAGAIN
			if cl.loglevel >= clusterrpc.LOGLEVEL_WARNINGS {
				cl.logger.Printf("[%s/%d] Timeout occurred (EAGAIN); retrying\n", cl.name, rqproto.GetSequenceNumber())
			}

			// Reconnect if there is only one peer (otherwise send() times out b/c the peer
			// is removed from the set in the REQ socket)
			if len(cl.raddr) == 1 {
				cl.createChannel()
			}

			msg, next_err := cl.sendRequest(rqproto, retries_left-1)

			if next_err != nil {
				return nil, next_err
			} else {
				return msg, nil
			}

		} else if 11 == uint32(err.(zmq.Errno)) { // We have no retries left.
			if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
				cl.logger.Printf("[%s/%d] Timeout occurred, retries failed. Giving up\n", cl.name, rqproto.GetSequenceNumber())
			}
			return nil, RequestError{status: proto.RPCResponse_STATUS_TIMEOUT, message: err.Error()}
		} else {
			if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
				cl.logger.Printf("[%s/%d] Network error: %s\n", cl.name, rqproto.GetSequenceNumber(), err.Error())
			}
			return nil, RequestError{status: proto.RPCResponse_STATUS_CLIENT_NETWORK_ERROR, message: err.Error()}
		}
	}
	if cl.loglevel >= clusterrpc.LOGLEVEL_DEBUG {
		cl.logger.Printf("[%s/%d] Received response from %s\n", cl.name, rqproto.GetSequenceNumber(), rqproto.GetSrvc()+"."+rqproto.GetProcedure())
	}

	return msg, nil
}
