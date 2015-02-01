package clusterrpc

type LOGLEVEL_T int

const (
	// Log absolutely nothing
	LOGLEVEL_NONE LOGLEVEL_T = iota
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
