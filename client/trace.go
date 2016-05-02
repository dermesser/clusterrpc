package client

import (
	"bytes"
	"clusterrpc/proto"
	"fmt"
	"strings"
	"time"
)

// Utility functions

const TRACE_INFO_TIME_FORMAT = "Mon Jan _2 15:04:05.999 2006"

/*
Formats the TraceInfo data structure.
*/
func FormatTraceInfo(ti *proto.TraceInfo, indent int) string {
	if ti == nil {
		return ""
	}
	indent_string := strings.Repeat(" ", indent)
	buf := bytes.NewBuffer(nil)

	fmt.Fprintf(buf, "%sReceived: %s\n", indent_string,
		time.Unix(0, int64(1000*ti.GetReceivedTime())).UTC().Format(TRACE_INFO_TIME_FORMAT))

	fmt.Fprintf(buf, "%sReplied: %s\n", indent_string,
		time.Unix(0, int64(1000*ti.GetRepliedTime())).UTC().Format(TRACE_INFO_TIME_FORMAT))

	if ti.GetMachineName() != "" {
		fmt.Fprintf(buf, "%sMachine: %s\n", indent_string, ti.GetMachineName())
	}
	if ti.GetEndpointName() != "" {
		fmt.Fprintf(buf, "%sEndpoint: %s\n", indent_string, ti.GetEndpointName())
	}
	if ti.GetErrorMessage() != "" {
		fmt.Fprintf(buf, "%sError: %s\n", indent_string, ti.GetErrorMessage())
	}
	if ti.GetRedirect() != "" {
		fmt.Fprintf(buf, "%sRedirect: %s\n", indent_string, ti.GetRedirect())
	}

	if len(ti.GetChildCalls()) > 0 {
		for _, call_traceinfo := range ti.GetChildCalls() {
			fmt.Fprintf(buf, FormatTraceInfo(call_traceinfo, indent+2))
			fmt.Fprintf(buf, "\n")
		}
	}

	return buf.String()
}
