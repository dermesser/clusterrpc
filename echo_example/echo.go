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

func echoHandler(cx *clusterrpc.Context) {
	cx.Success(cx.GetInput())
	fmt.Println("Called echoHandler:", string(cx.GetInput()), len(cx.GetInput()))
	return
}

func errorReturningHandler(cx *clusterrpc.Context) {
	cx.Fail("Some error occurred in handler, abort")
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
	srv := clusterrpc.NewServer("localhost", 9000)
	srv.SetLoglevel(clusterrpc.LOGLEVEL_DEBUG)
	srv.RegisterEndpoint("EchoService", "Echo", echoHandler)
	srv.RegisterEndpoint("EchoService", "Error", errorReturningHandler)
	srv.RegisterEndpoint("EchoService", "Redirect", redirectHandler)

	e := srv.AcceptRequests()

	if e != nil {
		fmt.Println(e.Error())
	}
}

func client() {
	cl, err := clusterrpc.NewClient("echo1_cl", "localhost", 9000)
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
	/// Async test
	cb := func(rsp []byte, err error) {
		if err != nil {
			fmt.Println("Error:", err.Error())
			return
		}
		fmt.Println(string(rsp))
	}
	cl.RequestAsync([]byte("Printed by callback"), "EchoService", "Echo", cb)

	time.Sleep(time.Second * 2) // Wait for callback
}

func main() {

	var srv, cl bool
	flag.BoolVar(&srv, "srv", false, "Specify if you want us to run as server")
	flag.BoolVar(&cl, "cl", false, "Specify if you want us to run as client")

	flag.Parse()

	if (srv && cl) || (!srv && !cl) {
		fmt.Println("Wrong combination: Use either -srv or -cl")
		return
	}

	if srv {
		server()
	}
	if cl {
		client()
	}

}
