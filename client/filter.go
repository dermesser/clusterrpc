package client

import (
	"clusterrpc/proto"
	"fmt"
	"time"
)

// A ClientFilter is a function that is called with a request and fulfills a certain task.
// Filters are stacked in Client.filters; filters[0] is called first, and calls in turn filters[1]
// until the last filter sends the message off to the network.
type ClientFilter (func(rq *Request, next_filter int) Response)

// TODO: Add RedirectFilter
var default_filters = []ClientFilter{TraceMergeFilter, TimeoutFilter, RetryFilter, SendFilter}

// Appends the received trace info to context or requested trace.
func TraceMergeFilter(rq *Request, next int) Response {
	response := rq.callNextFilter(next)

	if response.response != nil {
		if rq.trace != nil {
			*rq.trace = *response.response.GetTraceinfo()
		}
		if rq.ctx != nil {
			rq.ctx.AppendCallTrace(response.response.GetTraceinfo())
		}
	}
	return response
}

// Sets appropriate timeouts on the socket, only for this request
func TimeoutFilter(rq *Request, next int) Response {
	old_timeout, err := rq.client.channel.channel.GetRcvtimeo()

	if err == nil {
		if rq.ctx != nil && !rq.ctx.GetDeadline().IsZero() {
			rq.client.channel.SetTimeout(rq.ctx.GetDeadline().Sub(time.Now()))
		} else {
			rq.client.channel.SetTimeout(rq.params.timeout)
		}
		defer rq.client.channel.SetTimeout(old_timeout)
	}

	return rq.callNextFilter(next)
}

// Implements redirects: A server can tell us to follow a redirect. This is
// expensive in general because it involves setting up and tearing down a
// completely new client. It also doesn't work well with security-enabled
// RPCs.
func RedirectFilter(rq *Request, next int) Response {
	// NOTE: This filter is unimplemented, because it is being phased out. Redirections are an unnecessary feature.
	return rq.callNextFilter(next)
}

// A filter that retries a request according to the request's parameters.
func RetryFilter(rq *Request, next int) Response {
	attempts := int(rq.params.retries + 1)

	last_response := Response{}
	for i := 0; i < attempts; i++ {
		response := rq.callNextFilter(next)

		if response.err == nil {
			return response
		}
		last_response = response
		rq.client.channel.Reconnect()
	}
	return Response{err: fmt.Errorf("Retried %d times without success: %s", rq.params.retries, last_response.err.Error())}
}

// Send a request and wait for it to complete. Must be the last filter in the stack
func SendFilter(rq *Request, next int) Response {
	// Enforce that this is the last filter.
	if len(rq.client.filters) != next {
		panic("Bad filter setup")
	}

	message := rq.makeRPCRequestProto()
	payload, err := message.Marshal()

	if err != nil {
		panic("Could not serialize RPCRequest!!")
	}

	rq.client.last_sent = time.Now()
	err = rq.client.channel.sendMessage(payload)

	if err != nil {
		return Response{err: err}
	}

	response_payload, err := rq.client.channel.receiveMessage()

	if err != nil {
		return Response{err: err}
	}

	response := new(proto.RPCResponse)
	err = response.Unmarshal(response_payload)

	if err != nil {
		return Response{err: err}
	}

	return Response{response: response}
}
