/*
Use either as

	$ echo -srv

or

	$ echo -cl
*/
package main

import (
	"clusterrpc"
	"errors"
	"flag"
	"fmt"
)

func echoHandler(i []byte) (o []byte, e error) {
	o = i
	e = nil
	fmt.Println("Called echoHandler:", string(i), len(i))
	return
}

func errorReturningHandler(i []byte) (o []byte, err error) {
	err = errors.New("Some error occurred in handler, abort")
	o = nil
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

	///
	resp, err := cl.Request([]byte("helloworld"), "EchoService", "Echo")

	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Received response:", string(resp), len(resp))
	}
	///
	resp, err = cl.Request([]byte("helloworld"), "EchoService", "Error")

	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Received response:", string(resp), len(resp))
	}
	///
	resp, err = cl.Request([]byte("helloworld"), "EchoService", "Redirect")

	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Received response:", string(resp), len(resp))
	}
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
