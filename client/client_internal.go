package client

import (
	"clusterrpc"
	"clusterrpc/proto"
	"fmt"
	"time"

	pb "github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
)

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
	cl.channel.SetReconnectIvl(-1)

	for i := range cl.raddr {
		err = cl.channel.Connect(fmt.Sprintf("tcp://%s:%d", cl.raddr[i], cl.rport[i]))

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

	cl.channel.SetSndtimeo(cl.timeout)
	cl.channel.SetRcvtimeo(cl.timeout)
	cl.channel.SetReqCorrelate(1)
	cl.channel.SetReqRelaxed(1)

	return nil
}

func requestOneShot(raddr string, rport uint, service, endpoint string, request_data []byte, allow_redirect bool, settings_cl *Client) ([]byte, error) {
	var cl *Client
	var err error

	if settings_cl != nil {
		cl, err = NewClient(settings_cl.name+"_tmp", raddr, rport, settings_cl.loglevel)
	} else {
		cl, err = NewClient("anonymous_tmp_client", raddr, rport, clusterrpc.LOGLEVEL_WARNINGS)
	}

	if err != nil {
		return nil, RequestError{status: proto.RPCResponse_STATUS_CLIENT_REQUEST_ERROR, message: err.Error()}
	}

	defer cl.Close()

	cl.accept_redirect = allow_redirect

	if settings_cl != nil {
		cl.loglevel = settings_cl.loglevel
		cl.logger = settings_cl.logger
		cl.SetTimeout(settings_cl.timeout)
	}

	rsp, err := cl.Request(request_data, service, endpoint)

	if err != nil {
		return rsp, err
	}

	return rsp, nil
}

func (cl *Client) requestInternal(data []byte, service, endpoint string, retries_left int) ([]byte, error) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	rqproto := proto.RPCRequest{}

	rqproto.SequenceNumber = pb.Uint64(cl.sequence_number)
	cl.sequence_number++

	rqproto.Srvc = pb.String(service)
	rqproto.Procedure = pb.String(endpoint)
	rqproto.Data = data
	rqproto.CallerId = pb.String(cl.name)

	if cl.timeout > 0 {
		rqproto.Deadline = pb.Int64(time.Now().Unix() + int64(cl.timeout.Seconds()))
	}

	rq_serialized, pberr := pb.Marshal(&rqproto)

	if pberr != nil {
		if cl.loglevel >= clusterrpc.LOGLEVEL_WARNINGS {
			cl.logger.Println("PB error!", pberr.Error())
		}
		return nil, RequestError{status: proto.RPCResponse_STATUS_CLIENT_REQUEST_ERROR, message: pberr.Error()}
	}

	_, err := cl.channel.SendBytes(rq_serialized, 0)

	if err != nil {
		if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
			cl.logger.Printf("[%s/%d] Could not send message to %s. Error: %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint, err.Error())
		}
		if err.(zmq.Errno) == 11 { // EAGAIN
			return nil, RequestError{status: proto.RPCResponse_STATUS_TIMEOUT, message: err.Error()}
		} else {
			return nil, RequestError{status: proto.RPCResponse_STATUS_CLIENT_NETWORK_ERROR, message: err.Error()}
		}
	} else {
		if cl.loglevel >= clusterrpc.LOGLEVEL_DEBUG {
			cl.logger.Printf("[%s/%d] Sent request to %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint)
		}
	}

	msg, err := cl.channel.RecvBytes(0)

	if err != nil {
		if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
			cl.logger.Printf("[%s/%d] Could not receive response from %s, error %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint, err.Error())
		}
		if 11 == uint32(err.(zmq.Errno)) && retries_left > 0 { // 11 == EAGAIN
			if cl.loglevel >= clusterrpc.LOGLEVEL_WARNINGS {
				cl.logger.Printf("[%s/%d] Timeout occurred (EAGAIN); retrying\n", cl.name, rqproto.GetSequenceNumber())
			}

			// Do next request; timed-out socket will be disconnected and possibly reconnected
			cl.lock.Unlock()
			msg, next_err := cl.requestInternal(data, service, endpoint, retries_left-1)
			cl.lock.Lock()

			if next_err != nil {
				return nil, RequestError{status: proto.RPCResponse_STATUS_TIMEOUT, message: err.Error()}
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
			// Create new channel, old one is "confused" (REQ has an FSM internally allowing only req/rep/req/rep...)
			cl.channel.Close()
			cl.createChannel()
			return nil, RequestError{status: proto.RPCResponse_STATUS_CLIENT_NETWORK_ERROR, message: err.Error()}
		}
	}
	if cl.loglevel >= clusterrpc.LOGLEVEL_DEBUG {
		cl.logger.Printf("[%s/%d] Received response from %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint)
	}

	respproto := proto.RPCResponse{}

	err = pb.Unmarshal(msg, &respproto)

	if err != nil {
		if cl.loglevel >= clusterrpc.LOGLEVEL_ERRORS {
			cl.logger.Printf("[%s/%d] Error when unmarshaling response: %s\n", cl.name, rqproto.GetSequenceNumber(), err.Error())
		}
		return nil, RequestError{status: proto.RPCResponse_STATUS_CLIENT_REQUEST_ERROR, message: err.Error()}
	}

	if respproto.GetResponseStatus() != proto.RPCResponse_STATUS_OK && respproto.GetResponseStatus() != proto.RPCResponse_STATUS_REDIRECT {
		if cl.loglevel >= clusterrpc.LOGLEVEL_WARNINGS {
			cl.logger.Printf("[%s/%d] Received status other than ok from %s: %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint, statusToString(respproto.GetResponseStatus()))
		}
		return nil, RequestError{status: respproto.GetResponseStatus(), message: respproto.GetErrorMessage()}
	} else if respproto.GetResponseStatus() == proto.RPCResponse_STATUS_REDIRECT {
		if cl.accept_redirect {
			if respproto.GetRedirService() == "" || respproto.GetRedirEndpoint() == "" { // No different service.endpoint given, retry with same method
				return requestOneShot(respproto.GetRedirHost(), uint(respproto.GetRedirPort()), service, endpoint, data, false, cl)
			} else {
				return requestOneShot(respproto.GetRedirHost(), uint(respproto.GetRedirPort()), respproto.GetRedirService(), respproto.GetRedirEndpoint(), data, false, cl)
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
