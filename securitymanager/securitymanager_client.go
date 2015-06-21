package securitymanager

import (
	"errors"

	"github.com/pebbe/zmq4"
)

type ClientSecurityManager struct {
	*keyWriteLoader
	server_public string
}

// Sets up the manager and generates a new client key pair.
// The server public key is not yet loaded!
func NewClientSecurityManager() *ClientSecurityManager {
	mgr := &ClientSecurityManager{}
	var err error

	mgr.keyWriteLoader = new(keyWriteLoader)
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

	if err == nil && t != zmq4.REQ && t != zmq4.DEALER && t != zmq4.SUB {
		return errors.New("Wrong socket type (not DEALER, REQ, SUB)")
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
	kwl := new(keyWriteLoader)

	err := kwl.LoadKeys(keyfile, DONOTREAD)

	if err != nil {
		return err
	}

	mgr.server_public = kwl.public

	return nil
}

// Set the client key pair to the specified keys.
func (mgr *ClientSecurityManager) SetKeys(public, private string) {
	mgr.public, mgr.private = public, private
}
