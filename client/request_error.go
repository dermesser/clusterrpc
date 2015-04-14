package client

import "clusterrpc/proto"

type RequestError struct {
	status proto.RPCResponse_Status
	err    error
}

func (e RequestError) Error() string {
	if e.err != nil {
		return e.status.String() + ": " + e.err.Error()
	} else {
		return e.Status()
	}
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
	STATUS_MISSED_DEADLINE (the RPC server started processing the request after the deadline was already over)
	STATUS_LOADSHED (the RPC server is not willing to request any more requests right now)
	STATUS_UNHEALTHY (if health checking is enabled: The RPC server failed the health check)

The original error message can be retrieved with Message(). Use the idiom err.(*RequestError).Status() to obtain the status string.

*/
func (e *RequestError) Status() string {
	return e.status.String()
}

/*
Returns a human-readable error message such as "error resource temporarily unavailable" (which is an
EAGAIN error)
*/
func (e *RequestError) Message() string {
	return e.err.Error()
}
