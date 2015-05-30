/*
Clusterrpc is a server/client library for doing RPC. It uses protocol buffers over ZeroMQ for transport,
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

You probably need to install libzeromq >= 4 before being able to build clusterrpc.

For the architecture, especially the ZeroMQ patterns used, refer to
https://docs.google.com/drawings/d/1rERuS_D-5gAr8ImQ4kTPrDeZoEmOHNnG-WCSiLI5kUw/edit?usp=sharing

As of 25fe4b5, one client and one server both running on one processor (GOMAXPROCS=1;
Intel(R) Core(TM) i5 CPU       M 460  @ 2.53GHz) via localhost were capable of supporting between 5350
requests per second (with 35 bytes of payload) and 6850 requests per second (1 byte payload). This is
not good, but also not bad, given the relatively complex routing and load balancing structure (which
supports stability and scalability, also in the face of more complex workloads).
The server scales with the number of processes (client and server with both GOMAXPROCS=2 reached 7900 RPS,
10500 RPS with server's GOMAXPROCS=4). If client and server run both with 12 processes, 25000QPS were achieved.

When trying to find a good number of threads, keep in mind that one server process runs at least
three threads: One ZeroMQ networking thread (capable of roughly 1 GB/s, according to ZeroMQ documentation),
one load-balancing thread (shuffling messages between the frontend socket and the worker threads) and
finally (of course) one worker thread. The load-balancer is usually the hottest thread, so calculate one core
for it. You probably shouldn't use more workers than cores in your machine, except when the workers
are doing blocking operations, then up to ten workers per core or even more make sense.

*/
package clusterrpc
