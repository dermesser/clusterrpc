/*
Clusterrpc is a server/client library for doing RPC. It uses protocol buffers for transport,
the user payload data has however no defined format, but is simply a sequence of bytes (it is however
recommended to use protocol buffers or a similar technique to encode structured data)

Clusterrpc works with Services and Endpoints. One Server (i.e. a process with a socket listening on
a port) can serve several Services ("scopes") with each Service having multiple Endpoints (procedures).
An endpoint is essentially a registered handler function receiving a Context, similar to popular
web frameworks. The Context provides functions to read the input data, redirect, return errors etc.

E.g.:

	Service LockService
		+ Endpoint LockService.Acquire
		+ Endpoint LockService.Release
		+ Endpoint LockService.IsLocked
*/
package clusterrpc
