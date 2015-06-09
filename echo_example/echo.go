/*
Use either as

	$ echo -srv

or

	$ echo -cl

When not using standard host (localhost) for binding the server, don't use '*' or '0.0.0.0'
as the server will redirect and connect to itself using this address. Therefore you have
to use an exact address, e.g. -host=192.168.1.23.
*/
package main

import (
	"clusterrpc"
	"clusterrpc/client"
	"clusterrpc/proto"
	"clusterrpc/server"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var port uint
var host string

var timed_out bool = false

func echoHandler(cx *server.Context) {
	cx.Success(cx.GetInput())
	fmt.Println("Called echoHandler:", string(cx.GetInput()), len(cx.GetInput()))
	return
}

func silentEchoHandler(cx *server.Context) {
	cx.Success(cx.GetInput())
	return
}

func errorReturningHandler(cx *server.Context) {
	cx.Fail("Some error occurred in handler, abort")
	if !timed_out {
		timed_out = true
		time.Sleep(7 * time.Second)
	}
	return
}

// Calls another procedure on the same server (itself)
func callingHandler(cx *server.Context) {
	cl, err := client.NewClient("caller", host, port, clusterrpc.LOGLEVEL_WARNINGS)

	if err != nil {
		cx.Success([]byte("heyho :'("))
		return
	}

	b, err := cl.RequestWithCtx(cx, []byte("xyz"), "EchoService", "Echo")

	if err != nil {
		cx.Success([]byte("heyho :''("))
		return
	}

	b, err = cl.RequestWithCtx(cx, []byte("xyz"), "EchoService", "Error")

	if err != nil {
		cx.Success([]byte("heyho :''("))
		return
	}

	cx.Success(b)
}

var i int32 = 0

func redirectHandler(cx *server.Context) {
	cx.RedirectEndpoint(host, port, "EchoService", "Echo")
}

func Server() {
	poolsize := uint(runtime.GOMAXPROCS(0))

	// minimum poolsize for some functions of this demo to work
	if poolsize < 2 {
		poolsize = 2
	}

	srv, err := server.NewServer(host, port, poolsize, clusterrpc.LOGLEVEL_DEBUG) // don't set GOMAXPROCS if you want to test the loadbalancer (for correct queuing)

	if err != nil {
		return
	}

	srv.SetLoglevel(clusterrpc.LOGLEVEL_DEBUG)

	hostname, err := os.Hostname()

	if err == nil {
		srv.SetMachineName(hostname)
	}

	srv.RegisterHandler("EchoService", "Echo", echoHandler)
	srv.RegisterHandler("EchoService", "Error", errorReturningHandler)
	srv.RegisterHandler("EchoService", "Redirect", redirectHandler)
	srv.RegisterHandler("EchoService", "CallOther", callingHandler)
	e := srv.Start()

	if e != nil {
		fmt.Println(e.Error())
	}
}

func CachedClient() {
	cc := client.NewConnCache("echo1_cc", clusterrpc.LOGLEVEL_DEBUG)

	cl, err := cc.Connect(host, port)

	if err != nil {
		fmt.Println(err.Error())
		return
	}
	cl.SetTimeout(5 * time.Second)
	cl.SetHealthcheck(true)

	cc.Return(&cl)

	for i := 0; i < 5; i++ {
		cl, err = cc.Connect(host, port)

		if err != nil {
			fmt.Println(err.Error())
			return
		}

		resp, err := cl.Request([]byte("helloworld"), "EchoService", "Echo", nil)

		if err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println("Received response:", string(resp), len(resp))
		}

		cc.Return(&cl)
	}
}

func Client() {
	cl, err := client.NewClient("echo1_cl", host, port, clusterrpc.LOGLEVEL_DEBUG)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer cl.Close()
	cl.SetLoglevel(clusterrpc.LOGLEVEL_DEBUG)
	cl.SetTimeout(5 * time.Second)
	cl.SetHealthcheck(false)
	cl.SetRetries(2)

	trace_info := new(proto.TraceInfo)

	/// Plain echo
	status := cl.IsHealthy()

	if !status {
		fmt.Println("Backend not healthy")
		return
	}

	resp, err := cl.Request([]byte("helloworld"), "EchoService", "Echo", trace_info)
	fmt.Println(client.FormatTraceInfo(trace_info, 0))

	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Received response:", string(resp), len(resp))
	}
	/// Times out on first try, returns an app error after
	resp, err = cl.Request([]byte("helloworld"), "EchoService", "Error", trace_info)
	fmt.Println(client.FormatTraceInfo(trace_info, 0))

	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Received response:", string(resp), len(resp))
	}

	/// Redirect us to somewhere else

	resp, err = cl.Request([]byte("helloworld"), "EchoService", "Redirect", trace_info)
	fmt.Println(client.FormatTraceInfo(trace_info, 0))

	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Received response:", string(resp), len(resp))
	}

	// NOT_FOUND

	resp, err = cl.Request([]byte("helloworld"), "EchoService", "DoesNotExist", trace_info)
	fmt.Println(client.FormatTraceInfo(trace_info, 0))

	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Received response:", string(resp), len(resp))
	}

	// Cascading function (calls other RPC)
	resp, err = cl.Request([]byte("helloworld"), "EchoService", "CallOther", trace_info)
	fmt.Println(client.FormatTraceInfo(trace_info, 0))

	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Received response:", string(resp), len(resp))
	}

}

func Aclient() {
	acl, err := client.NewAsyncClient("echo1_acl", host, port, 1, clusterrpc.LOGLEVEL_DEBUG)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer acl.Close()
	acl.SetLoglevel(clusterrpc.LOGLEVEL_DEBUG)

	callback := func(rp []byte, e error) {
		if e == nil {
			fmt.Println("Received response:", string(rp))
		} else {
			fmt.Println("Received error:", e.Error())
		}
	}

	/// Plain echo
	acl.Request([]byte("helloworld"), "EchoService", "Echo", callback)

	/// Return an app error
	acl.Request([]byte("helloworld"), "EchoService", "Error", callback)

	/// Redirect us to somewhere else
	acl.Request([]byte("helloworld"), "EchoService", "Redirect", callback)

	time.Sleep(3 * time.Second)
}

func benchServer() {
	poolsize := uint(runtime.GOMAXPROCS(0))

	// minimum poolsize for some functions of this demo to work
	if poolsize < 2 {
		poolsize = 2
	}

	srv, err := server.NewServer(host, port, poolsize, clusterrpc.LOGLEVEL_WARNINGS)

	if err != nil {
		return
	}

	srv.SetLoglevel(clusterrpc.LOGLEVEL_ERRORS)
	srv.RegisterHandler("EchoService", "Echo", silentEchoHandler)

	e := srv.Start()

	if e != nil {
		fmt.Println(e.Error())
	}
}

var requestcount uint32 = 0
var waitgroup sync.WaitGroup

func benchClient(n int) {
	cl, err := client.NewClient("echo1_cl", host, port, clusterrpc.LOGLEVEL_WARNINGS)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer cl.Close()
	cl.SetTimeout(5 * time.Second)
	waitgroup.Add(1)

	for i := 0; i < n; i++ {
		_, err := cl.Request([]byte("H"), "EchoService", "Echo", nil)

		if err != nil {
			fmt.Println("Client benchmark error:", err.Error())
		}
		atomic.AddUint32(&requestcount, 1)
	}
	waitgroup.Done()
}

func main() {

	var srv, cl, acl, srvbench bool
	var clbench int

	flag.BoolVar(&acl, "acl", false, "Run the asynchronous client")
	flag.BoolVar(&cl, "cl", false, "Run synchronous client")
	// Global var
	flag.StringVar(&host, "host", "127.0.0.1", "Remote host (hostname or IP)")
	// Global var
	flag.UintVar(&port, "port", 9000, "Remote port")
	flag.BoolVar(&srv, "srv", false, "Run server (localhost:9000)")
	flag.BoolVar(&srvbench, "srvbench", false, "Run benchmark server (LOGLEVEL_ERRORS, GOMAXPROCS threads, only Echo)")

	flag.IntVar(&clbench, "clbench", 0, "Run the benchmark client; there will be GOMAXPROCS clients running, each sending `clbench` requests.")

	flag.Parse()

	var argcnt int = 0
	if srv {
		argcnt++
	}
	if cl {
		argcnt++
	}
	if acl {
		argcnt++
	}
	if srvbench {
		argcnt++
	}
	if clbench > 0 {
		argcnt++
	}

	if argcnt != 1 {
		fmt.Println("Wrong combination: Use either -srv, -acl or -cl, -clbench or -srvbench")
		return
	}

	if srv {
		Server()
	} else if cl {
		CachedClient()
		Client()
	} else if acl {
		Aclient()
	} else if clbench > 0 {
		for i := 0; i < runtime.GOMAXPROCS(0)-1; i++ {
			go benchClient(clbench)
		}

		benchClient(clbench)
		waitgroup.Wait()
		fmt.Println(requestcount)

	} else if srvbench {
		benchServer()
	}

}
