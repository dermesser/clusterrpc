package client

import (
	"github.com/dermesser/clusterrpc/proto"

	pb "github.com/gogo/protobuf/proto"
)

type Response struct {
	err      error
	response *proto.RPCResponse
}

// Check whether the request was successful.
func (rp *Response) Ok() bool {
	return rp.err == nil && rp.response.GetResponseStatus() == proto.RPCResponse_STATUS_OK
}

// Returns the response payload.
func (rp *Response) Payload() []byte {
	return rp.response.GetResponseData()
}

// Unmarshals the response into msg.
func (rp *Response) GetResponseMessage(msg pb.Message) error {
	return pb.Unmarshal(rp.response.GetResponseData(), msg)
}

// Get the error that has occurred.
//
// Special codes are returned for RPC errors, which start with prefix "RPC:"
// and a code from the proto/rpc.proto enum RPCResponse.
func (rp *Response) Error() string {
	if rp.err != nil {
		return rp.err.Error()
	} else if rp.response.GetResponseStatus() != proto.RPCResponse_STATUS_OK {
		return "RPC:" + rp.response.GetResponseStatus().String()
	} else {
		return ""
	}
}
