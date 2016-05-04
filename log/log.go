package log

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"time"
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
	rand.Seed(time.Now().UnixNano())
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

// Performance-enhancer: Prevent unnecessary log calls
func IsLoggingEnabled(ll int) bool {
	return loglevel >= ll
}

func CRPC_log(ll int, what ...interface{}) {
	if ll <= loglevel {
		logger.Printf("%s: %s", loglevel_to_string(loglevel), fmt.Sprintln(what...))
	}
}

// Returns a short random alphanumeric string.
// This is used to assign special tokens to RPCs in order to track them across log lines.
func GetLogToken() string {
	str := make([]byte, 6)
	for i := range str {
		str[i] = byte(65 + (rand.Int() % 26))
	}
	return string(str)
}
