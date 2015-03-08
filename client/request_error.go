package client

import "clusterrpc/proto"

type RequestError struct {
	status  proto.RPCResponse_Status
	message string
}

func (e RequestError) Error() string {
	return statusToString(e.status) + ": " + e.message
}

/*
Returns one of

	STATUS_UNKNOWN (default value; this means that the RequestError object hasn't been initialized)
	STATUS_NOT_FOUND (the given service or endpoint couldn't be located)
	STATUS_NOT_OK (application handler returned with an error. Message() has an error message)
	STATUS_SERVER_ERROR (the clusterrpc peer had a problem, e.g. couldn't decode a protocol buffer)
	STATUS_TIMEOUT (either the attempts to receive or send a message timed out. Sending is tried only once, receiving as often as you have specified with SetRetries())
	STATUS_OVERLOADED_RETRY (the server is overloaded and wasn't able to even queue our request. Retry later or elsewhere)
	STATUS_CLIENT_REQUEST_ERROR (we failed to send or receive the request. Reason could be a Protobuf (de)serialization error, ...)
	STATUS_CLIENT_NETWORK_ERROR (the socket returned an unrecoverable error; e.g. we could not send a request in the first place)
	STATUS_CLIENT_CALLED_WRONG (a NewClientRR() call was made with host/port slices differing in length.

The original error message can be retrieved with Message().

*/
func (e *RequestError) Status() string {
	return statusToString(e.status)
}

/*
Returns a human-readable error message such as "error resource temporarily unavailable" (which is an
EAGAIN error)
*/
func (e *RequestError) Message() string {
	return e.message
}

func statusToString(s proto.RPCResponse_Status) string {
	return proto.RPCResponse_Status_name[int32(s)]
}
