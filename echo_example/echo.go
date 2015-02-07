/*
Use either as

	$ echo -srv

or

	$ echo -cl
*/
package main

import (
	"clusterrpc"
	"flag"
	"fmt"
	"time"
)

var timed_out bool = false

func echoHandler(cx *clusterrpc.Context) {
	cx.Success(cx.GetInput())
	fmt.Println("Called echoHandler:", string(cx.GetInput()), len(cx.GetInput()))
	return
}

func errorReturningHandler(cx *clusterrpc.Context) {
	cx.Fail("Some error occurred in handler, abort")
	if !timed_out {
		timed_out = true
		time.Sleep(20 * time.Second)
	}
	return
}

var i int32 = 0

func redirectHandler(cx *clusterrpc.Context) {
	if i%2 == 0 {
		cx.Redirect("localhost", 9000)
	} else {
		cx.Success([]byte("Hello from redirected!"))
	}
	i++
}

func server() {
	srv := clusterrpc.NewServer("127.0.0.1", 9000, 4)
	srv.SetLoglevel(clusterrpc.LOGLEVEL_DEBUG)
	srv.RegisterEndpoint("EchoService", "Echo", echoHandler)
	srv.RegisterEndpoint("EchoService", "Error", errorReturningHandler)
	srv.RegisterEndpoint("EchoService", "Redirect", redirectHandler)

	e := srv.Start()

	if e != nil {
		fmt.Println(e.Error())
	}
}

func client() {
	cl, err := clusterrpc.NewClient("echo1_cl", "127.0.0.1", 9000)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer cl.Close()
	cl.SetLoglevel(clusterrpc.LOGLEVEL_DEBUG)

	/// Plain echo
	resp, err := cl.Request([]byte("helloworld"), "EchoService", "Echo")

	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Received response:", string(resp), len(resp))
	}
	/// Return an app error
	resp, err = cl.Request([]byte("helloworld"), "EchoService", "Error")

	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Received response:", string(resp), len(resp))
	}

	/// Redirect us to somewhere else
	resp, err = cl.Request([]byte("helloworld"), "EchoService", "Redirect")

	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Received response:", string(resp), len(resp))
	}

	// NOT_FOUND

	resp, err = cl.Request([]byte("helloworld"), "EchoService", "DoesNotExist")

	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Received response:", string(resp), len(resp))
	}
}

func aclient() {
	acl, err := clusterrpc.NewAsyncClient("echo1_acl", "127.0.0.1", 9000, 1)
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

func main() {

	var srv, cl, acl, both bool
	flag.BoolVar(&srv, "srv", false, "Specify if you want us to run as server")
	flag.BoolVar(&cl, "cl", false, "Specify if you want us to run as client")
	flag.BoolVar(&acl, "acl", false, "Specify if you want us to run as asynchronous client")
	flag.BoolVar(&both, "both", false, "Specify if you want us to run server and client in one process")

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
	if both {
		argcnt++
	}

	if argcnt != 1 {
		fmt.Println("Wrong combination: Use either -srv, -acl or -cl")
		return
	}

	if srv {
		server()
	} else if cl {
		client()
	} else if acl {
		aclient()
	} else if both {

		go server()

		time.Sleep(2 * time.Second)

		client()

	}

}
