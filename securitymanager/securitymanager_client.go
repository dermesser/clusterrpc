package securitymanager

import (
	"errors"

	"github.com/pebbe/zmq4"
)

// ClientSecurityManager manages encryption for client sockets.
type ClientSecurityManager struct {
	// Provides LoadKeys/WriteKeys functionality
	*keyWriteLoader
	// Public key of the server to connect to.
	serverPublic string
}

// NewClientSecurityManager sets up the manager and generates a new client key pair.
//
// In order to connect to a server, the server's public key must be set before creating a
// client. Otherwise, the connection will not succeed.
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

// For internal use: ApplyToClientSocket sets up a client socket for CURVE security. If
// called on nil, does nothing. This function must be called before calling Connect() on
// the socket!
func (mgr *ClientSecurityManager) ApplyToClientSocket(sock *zmq4.Socket) error {
	if mgr == nil {
		return nil
	}

	if mgr.serverPublic == "" || mgr.public == "" || mgr.private == "" {
		return errors.New("Not all three keys (server's public, client public, client private) are set")
	}

	t, err := sock.GetType()

	if err == nil && t != zmq4.REQ && t != zmq4.DEALER && t != zmq4.SUB {
		return errors.New("Wrong socket type (not DEALER, REQ, SUB)")
	} else if err != nil {
		return err
	}

	err = sock.ClientAuthCurve(mgr.serverPublic, mgr.public, mgr.private)

	if err != nil {
		return err
	}

	return nil
}

// SetServerPubkey sets the public key of the server. This is required to be able to
// connect to a server using a secure connection.
func (mgr *ClientSecurityManager) SetServerPubkey(key string) {
	mgr.serverPublic = key
}

// LoadServerPubkey loads the public key of the server from the specified file.
func (mgr *ClientSecurityManager) LoadServerPubkey(keyfile string) error {
	kwl := new(keyWriteLoader)

	err := kwl.LoadKeys(keyfile, DONOTREAD)

	if err != nil {
		return err
	}

	mgr.serverPublic = kwl.public

	return nil
}

// SetKeys sets the client key pair to the specified keys.
func (mgr *ClientSecurityManager) SetKeys(public, private string) {
	mgr.public, mgr.private = public, private
}
