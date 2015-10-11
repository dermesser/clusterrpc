package client

import (
	"fmt"
	"strings"

	pb "github.com/gogo/protobuf/proto"
)

type rpclog_type int

const (
	log_REQUEST rpclog_type = iota
	log_RESPONSE
	log_ERROR
)

func (t rpclog_type) String() string {
	switch t {
	case log_REQUEST:
		return "REQ"
	case log_RESPONSE:
		return "RSP"
	case log_ERROR:
		return "ERR"
	default:
		return ""
	}
}

func transformRuneToPrintable(r rune) rune {
	if r >= 0 && r < 32 {
		return '.'
	} else if r >= 32 && r < 127 {
		return r
	} else if r >= 127 {
		return '.'
	} else {
		return '.'
	}
}

func logString(str []byte) string {
	return strings.Map(transformRuneToPrintable, string(str))
}

func logProtobuf(p pb.Message) string {
	return p.String()
}

func (cl *Client) connIdString() string {
	if len(cl.raddr) < 2 && len(cl.raddr) > 0 {
		return fmt.Sprintf("%s/%d->%s%d:", cl.name, cl.sequence_number, cl.raddr[0], cl.rport[0])
	} else if len(cl.raddr) > 1 {
		return fmt.Sprintf("%s/%d->%v/%v:", cl.name, cl.sequence_number, cl.raddr, cl.rport)
	} else {
		return ""
	}
}

func (cl *Client) rpclogErr(service, endpoint string, err error) {
	if cl.rpclogger != nil {
		cl.rpclogger.Println(log_ERROR.String(), err.Error())
	}
}

func (cl *Client) rpclogPB(service, endpoint string, p pb.Message, t rpclog_type) {
	if cl.rpclogger != nil {
		str := logProtobuf(p)

		cl.rpclogger.Println(t.String(), cl.connIdString(), str)
	}
}

func (cl *Client) rpclogRaw(service, endpoint string, b []byte, t rpclog_type) {
	if cl.rpclogger != nil {
		cl.rpclogger.Println(t.String(), cl.connectToPeers(), logString(b))
	}
}

func (cl *Client) rpclogStr(service, endpoint string, s string, t rpclog_type) {
	if cl.rpclogger != nil {
		cl.rpclogger.Println(t.String(), cl.connIdString(), s)
	}
}
