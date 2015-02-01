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
)

func echoHandler(i []byte) (o []byte, e error) {
	o = i
	e = nil
	fmt.Println("Called echoHandler:", string(i), len(i))
	return
}

func server() {
	srv := clusterrpc.NewServer("localhost", 9000)
	srv.RegisterEndpoint("EchoService", "Echo", echoHandler)

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

	resp, err := cl.Request([]byte("helloworld"), "EchoService", "Echo")

	if err != nil {
		fmt.Println(err.Error())
		return
	} else {
		fmt.Println("Received response:", string(resp), len(resp))
		cl.Close()
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
