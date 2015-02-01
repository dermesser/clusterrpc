package clusterrpc

import "clusterrpc/proto"

type RequestError struct {
	status  proto.RPCResponse_Status
	message string
}

func (e RequestError) Error() string {
	return StatusToString(e.status) + ": " + e.message
}

func (e *RequestError) Status() string {
	return StatusToString(e.status)
}

func (e *RequestError) Message() string {
	return e.message
}

func StatusToString(s proto.RPCResponse_Status) string {
	return proto.RPCResponse_Status_name[int32(s)]
}
