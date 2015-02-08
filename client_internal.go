package clusterrpc

import (
	"clusterrpc/proto"
	"errors"
	"fmt"
	"time"

	pb "code.google.com/p/goprotobuf/proto"
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
		return err
	}

	err = cl.channel.Connect(fmt.Sprintf("tcp://%s:%d", cl.raddr, cl.rport))

	if err != nil {
		cl.logger.Println("Error when connecting Req socket:", err.Error())
		return err
	}

	cl.channel.SetSndtimeo(cl.timeout)
	cl.channel.SetRcvtimeo(cl.timeout)

	return nil
}

func requestOneShot(raddr string, rport uint32, service, endpoint string, request_data []byte, allow_redirect bool, settings_cl *Client) ([]byte, error) {
	var cl *Client
	var err error

	if settings_cl != nil {
		cl, err = NewClient(settings_cl.name+"_tmp", raddr, rport)
	} else {
		cl, err = NewClient("anonymous_tmp_client", raddr, rport)
	}

	if err != nil {
		return nil, err
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
	rqproto.Data = pb.String(string(data))
	rqproto.CallerId = pb.String(cl.name)

	if cl.timeout > 0 {
		rqproto.Deadline = pb.Int64(time.Now().Unix() + int64(cl.timeout.Seconds()))
	}

	rq_serialized, pberr := pb.Marshal(&rqproto)

	if pberr != nil {
		if cl.loglevel >= LOGLEVEL_WARNINGS {
			cl.logger.Println("PB error!", pberr.Error())
		}
		return nil, pberr
	}

	_, err := cl.channel.SendBytes(rq_serialized, 0)

	if err != nil {
		if cl.loglevel >= LOGLEVEL_ERRORS {
			cl.logger.Printf("[%s/%d] Could not send message to %s. Error: %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint, err.Error())
		}
		return nil, err
	} else {
		if cl.loglevel >= LOGLEVEL_DEBUG {
			cl.logger.Printf("[%s/%d] Sent request to %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint)
		}
	}

	msg, err := cl.channel.RecvBytes(0)

	if err != nil {
		if cl.loglevel >= LOGLEVEL_ERRORS {
			cl.logger.Printf("[%s/%d] Could not receive response from %s, error %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint, err.Error())
		}
		if 11 == uint32(err.(zmq.Errno)) && retries_left > 0 { // 11 == EAGAIN
			if cl.loglevel >= LOGLEVEL_WARNINGS {
				cl.logger.Printf("[%s/%d] Timeout occurred (EAGAIN); retrying\n", cl.name, rqproto.GetSequenceNumber())
			}

			// Create new channel, old one is "confused" (REQ has an FSM internally allowing only req/rep/req/rep...)
			cl.createChannel()
			cl.lock.Unlock()
			msg, next_err := cl.requestInternal(data, service, endpoint, retries_left-1)
			cl.lock.Lock()

			if next_err != nil {
				return nil, next_err
			} else {
				return msg, nil
			}

		}
		return nil, err
	}
	if cl.loglevel >= LOGLEVEL_DEBUG {
		cl.logger.Printf("[%s/%d] Received response from %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint)
	}

	respproto := proto.RPCResponse{}

	err = pb.Unmarshal(msg, &respproto)

	if err != nil {
		if cl.loglevel >= LOGLEVEL_ERRORS {
			cl.logger.Printf("[%s/%d] Error when unmarshaling response: %s\n", cl.name, rqproto.GetSequenceNumber(), err.Error())
		}
		return nil, err
	}

	if respproto.GetResponseStatus() != proto.RPCResponse_STATUS_OK && respproto.GetResponseStatus() != proto.RPCResponse_STATUS_REDIRECT {
		if cl.loglevel >= LOGLEVEL_WARNINGS {
			cl.logger.Printf("[%s/%d] Received status other than ok from %s: %s\n", cl.name, rqproto.GetSequenceNumber(), service+"."+endpoint, StatusToString(respproto.GetResponseStatus()))
		}
		err = RequestError{status: respproto.GetResponseStatus(), message: respproto.GetErrorMessage()}
		return nil, err
	} else if respproto.GetResponseStatus() == proto.RPCResponse_STATUS_REDIRECT {
		if cl.accept_redirect {
			return requestOneShot(respproto.GetRedirHost(), respproto.GetRedirPort(), service, endpoint, data, false, cl)
		} else {
			return nil, errors.New("Could not follow redirect (redirect loop avoidance)")
		}
	}

	return []byte(respproto.GetResponseData()), nil
}
