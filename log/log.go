package log

import (
	"fmt"
	"io"
	"log"
	"os"
)

const (
	// Log absolutely nothing
	LOGLEVEL_NONE int = iota
	// Log situations that are not expected to happen and
	// are difficult to handle (e.g. by closing the connection without further consideration)
	LOGLEVEL_ERRORS
	// Log non-critical situations that might happen, but shouldn't (e.g. returning STATUS_NOT_FOUND)
	LOGLEVEL_WARNINGS
	// Log situations that are expected, but important for the operation
	LOGLEVEL_INFO
	// Log everything
	LOGLEVEL_DEBUG
)

func init() {
	logger = log.New(os.Stderr, "clusterrpc ", logger_flags)
}

var logger *log.Logger
var loglevel int

const logger_flags = log.LstdFlags | log.Lmicroseconds

var loglevel_strings []string = []string{"[NON]", "[ERR]", "[WRN]", "[INF]", "[DBG]"}

// Set the global RPC logging device
func SetLoggingDevice(out io.Writer) {
	logger = log.New(out, "clusterrpc ", logger_flags)
}

func loglevel_to_string(loglevel int) string {
	return loglevel_strings[loglevel]
}

// Set the global RPC log level
func SetLoglevel(ll int) {
	loglevel = ll
}

func CRPC_log(ll int, what ...interface{}) {
	if ll <= loglevel {
		logger.Printf("%s: %s", loglevel_to_string(loglevel), fmt.Sprintln(what...))
	}
}
