package main

import (
	"clusterrpc/client"
	"flag"
	"fmt"
)

func main() {
	fmt.Println("Generating key pair...")

	var pubfile, privfile string

	flag.StringVar(&pubfile, "pub", "publickey.txt", "File to write public key to.")
	flag.StringVar(&privfile, "priv", "privatekey.txt", "File to write private key to.")

	flag.Parse()

	mgr := client.NewClientSecurityManager()

	mgr.WriteKeys(pubfile, privfile)

	return
}
