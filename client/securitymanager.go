package client

import (
	"bytes"
	"errors"
	"os"

	"github.com/pebbe/zmq4"
)

const DONOTWRITE = "___donotwrite_this_key"
const DONOTREAD = "___donotread_key_from_file"

type ClientSecurityManager struct {
	public, private string
	server_public   string
}

// Sets up the manager and generates a new client key pair.
// The server public key is not yet loaded!
func NewClientSecurityManager() *ClientSecurityManager {
	mgr := &ClientSecurityManager{}
	var err error

	mgr.public, mgr.private, err = zmq4.NewCurveKeypair()

	if err != nil {
		return nil
	}

	return mgr
}

// Sets up a client socket for CURVE security. If called on nil, does nothing.
// This function must be called before calling Connect() on the socket!
func (mgr *ClientSecurityManager) ApplyToClientSocket(sock *zmq4.Socket) error {
	if mgr == nil {
		return nil
	}

	if mgr.server_public == "" || mgr.public == "" || mgr.private == "" {
		return errors.New("Not all three keys (server's public, client public, client private) are set")
	}

	t, err := sock.GetType()

	if err == nil && t != zmq4.REQ && t != zmq4.DEALER {
		return errors.New("Wrong socket type (not DEALER, REQ)")
	} else if err != nil {
		return err
	}

	err = sock.ClientAuthCurve(mgr.server_public, mgr.public, mgr.private)

	if err != nil {
		return err
	}

	return nil
}

// Set the public key of the server.
func (mgr *ClientSecurityManager) SetServerPubkey(key string) {
	mgr.server_public = key
}

// Load the public key of the server from the specified file.
func (mgr *ClientSecurityManager) LoadServerPubkey(keyfile string) error {
	pubfile, err := os.Open(keyfile)
	defer pubfile.Close()

	if err != nil {
		return err
	}

	pubkeybuf := bytes.NewBuffer(nil)

	n, err := pubkeybuf.ReadFrom(pubfile)

	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("Could not read public key")
	}

	mgr.server_public = pubkeybuf.String()

	return nil
}

// Set the client key pair to the specified keys.
func (mgr *ClientSecurityManager) SetKeys(public, private string) {
	mgr.public, mgr.private = public, private
}

// Load the client key pair from the specified files.
func (mgr *ClientSecurityManager) LoadKeys(public_file, private_file string) error {
	pubfile, err := os.Open(public_file)
	defer pubfile.Close()

	if err != nil {
		return err
	}

	pubkeybuf, privkeybuf := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

	n, err := pubkeybuf.ReadFrom(pubfile)

	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("Could not read public key")
	}

	privfile, err := os.Open(private_file)
	defer privfile.Close()

	if err != nil {
		return err
	}

	n, err = privkeybuf.ReadFrom(privfile)

	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("Could not read private key")
	}

	mgr.private = string(privkeybuf.Bytes())
	mgr.public = string(pubkeybuf.Bytes())
	return nil
}

// Write the client key pair to the specified files. If one of the file names is
// client.DONOTWRITE, the function does not write the key to a file.
func (mgr *ClientSecurityManager) WriteKeys(public_file, private_file string) error {
	if public_file != DONOTWRITE {
		pubfile, err := os.OpenFile(public_file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		defer pubfile.Close()

		if err != nil {
			return err
		}

		n, err := pubfile.Write([]byte(mgr.public))

		if err != nil {
			return err
		}
		if n != len(mgr.public) {
			return errors.New("Could not write correct number of bytes to public key file")
		}

	}

	if private_file != DONOTWRITE {
		privfile, err := os.OpenFile(private_file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		defer privfile.Close()

		if err != nil {
			return err
		}

		n, err := privfile.Write([]byte(mgr.private))

		if err != nil {
			return err
		}
		if n != len(mgr.private) {
			return errors.New("Could not write correct number of bytes to private key file")
		}
	}

	return nil
}
