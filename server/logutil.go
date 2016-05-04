package server

import (
	"fmt"
	"strings"
	"time"

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

func (ctx *Context) connIdString(size int) string {
	dead_left := (ctx.orig_rq.GetDeadline() / 1000) - (time.Now().UnixNano() / 1000000)

	if ctx.orig_rq.GetDeadline() == 0 {
		dead_left = 0
	}

	return fmt.Sprintf("%s.%s %s/%s %d B [%d ms left]", ctx.orig_rq.GetSrvc(), ctx.orig_rq.GetProcedure(),
		ctx.orig_rq.GetCallerId(), ctx.orig_rq.GetRpcId(),
		size, dead_left)
}

func (ctx *Context) rpclogErr(err error) {
	if ctx.logger != nil {
		ctx.logger.Println(log_ERROR.String(), err.Error())
	}
}

func (ctx *Context) rpclogPB(p pb.Message, t rpclog_type) {
	if ctx.logger != nil {
		if (ctx.log_state == 0 && t == log_REQUEST) ||
			(ctx.log_state == 1 && t == log_RESPONSE) {

			str := logProtobuf(p)

			ctx.logger.Println(t.String(), ctx.connIdString(pb.Size(p)), str)
			ctx.log_state++
		}
	}
}

func (ctx *Context) rpclogRaw(b []byte, t rpclog_type) {
	if ctx.logger != nil {
		if (ctx.log_state == 0 && t == log_REQUEST) ||
			(ctx.log_state == 1 && t == log_RESPONSE) {

			ctx.logger.Println(t.String(), ctx.connIdString(len(b)), logString(b))
			ctx.log_state++
		}
	}
}

func (ctx *Context) rpclogStr(s string, t rpclog_type) {
	if ctx.logger != nil {
		if (ctx.log_state == 0 && t == log_REQUEST) ||
			(ctx.log_state == 1 && t == log_RESPONSE) {

			ctx.logger.Println(t.String(), ctx.connIdString(len(s)), s)
			ctx.log_state++
		}
	}
}
