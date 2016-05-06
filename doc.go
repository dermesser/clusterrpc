/*
Clusterrpc is a server/client library for doing RPC. It uses protocol buffers
over ZeroMQ for transport, the user payload data has however no defined format,
but is simply a sequence of bytes (it is however recommended to use protocol
buffers or a similar technique to encode structured data)

Clusterrpc works with Services and Endpoints. One Server (i.e. a process with a
socket listening on a port) can serve several Services ("scopes") with each
Service having multiple Endpoints (procedures). An endpoint is essentially a
registered handler function receiving a Context, similar to popular web
frameworks. The Context provides functions to read the input data, redirect,
return errors etc.

E.g.:

	Service LockService
	    + Endpoint LockService.Acquire
	    + Endpoint LockService.Release
	    + Endpoint LockService.IsLocked

You probably need to install libzeromq-dev[el] >= 4 before being able to build
clusterrpc.

For the architecture, especially the ZeroMQ patterns used, refer to
https://docs.google.com/drawings/d/1rERuS_D-5gAr8ImQ4kTPrDeZoEmOHNnG-WCSiLI5kUw/edit?usp=sharing

When working in parallel -- i.e., dozens of clients sending requests to a
server with dozens of threads, throughput on the order of several tens of
thousands requests per second can be achieved.
*/
package clusterrpc
