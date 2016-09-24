package securitymanager

import (
	"errors"

	"github.com/pebbe/zmq4"
)

// DONOTWRITE can be used as file name if you don't want the key written to disk.
const DONOTWRITE = "___donotwrite_key_to_file"

// DONOTREAD can be used as file name if you don't want the key read from disk.
const DONOTREAD = "___donotread_key_from_file"

const serverDomain = "clusterrpc.srv"

// This module manages keys for a clusterrpc server. It is built after the API calls
// as shown in the Iron House example of ZeroMQs CURVE security documentation.

// ServerSecurityManager can be supplied to NewServer(). It then sets up encryption and
// authentication.
// The security manager is very easy to use and enables both cryptographic/CURVE security and authentication
// and (additionally - on top of that) IP authentication.
type ServerSecurityManager struct {
	*keyWriteLoader
	// Z85 keys
	allowedClientKeys []string

	// Only set one of both!
	allowedClientAddresses []string
	deniedClientAddresses  []string
}

// NewServerSecurityManager sets up a key manager and generates a new key pair.
func NewServerSecurityManager() *ServerSecurityManager {
	mgr := &ServerSecurityManager{}
	var err error

	mgr.keyWriteLoader = new(keyWriteLoader)
	mgr.public, mgr.private, err = zmq4.NewCurveKeypair()

	if err != nil {
		return nil
	}

	return mgr
}

// ApplyToServerSocket applies the internal keys to the server.
// This must be called before applying Bind() on the socket!
// Safe to call on a nil manager (nothing happens in that case
func (mgr *ServerSecurityManager) ApplyToServerSocket(sock *zmq4.Socket) error {
	if mgr == nil {
		return nil
	}

	if mgr.private == "" || mgr.public == "" {
		return errors.New("Incomplete initialization: No key(s)")
	}

	t, err := sock.GetType()

	// Only apply to actual server sockets
	if err == nil && t != zmq4.ROUTER && t != zmq4.REP && t != zmq4.PUB {
		return errors.New("Wrong socket type (not ROUTER, REP, PUB)")
	} else if err != nil {
		return err
	}
	// start in any case (returns error if already running, ignore that)
	zmq4.AuthStart()

	if mgr.allowedClientAddresses != nil {
		// We can use a static string because this is the only server in the context
		zmq4.AuthAllow(serverDomain, mgr.allowedClientAddresses...)
	} else if mgr.deniedClientAddresses != nil {
		zmq4.AuthDeny(serverDomain, mgr.deniedClientAddresses...)
	}

	if mgr.allowedClientKeys != nil {
		zmq4.AuthCurveAdd(serverDomain, mgr.allowedClientKeys...)
	} else {
		// Make it open
		zmq4.AuthCurveAdd(serverDomain, zmq4.CURVE_ALLOW_ANY)
	}

	err = sock.ServerAuthCurve(serverDomain, mgr.private)

	if err != nil {
		return err
	}

	return nil
}

// StopManager tears down all resources associated with authentication
func (mgr *ServerSecurityManager) StopManager() {
	zmq4.AuthStop()
}

// SetKeys sets the public and private keys of the server.
func (mgr *ServerSecurityManager) SetKeys(public, private string) {
	mgr.public, mgr.private = public, private
}

// GetPublicKey returns the public key of the server.
func (mgr *ServerSecurityManager) GetPublicKey() string {
	return mgr.public
}

// AddClientKeys adds keys of clients that are accepted.
func (mgr *ServerSecurityManager) AddClientKeys(keys ...string) {
	mgr.allowedClientKeys = append(mgr.allowedClientKeys, keys...)
}

// ResetClientKeys removes all clients from the whitelist, effectively enforcing an OPEN policy
func (mgr *ServerSecurityManager) ResetClientKeys() {
	mgr.allowedClientKeys = nil
}

// ResetBlackWhiteLists removes all clients from the blacklist, effectively enforcing an OPEN policy
func (mgr *ServerSecurityManager) ResetBlackWhiteLists() {
	mgr.allowedClientAddresses = nil
	mgr.deniedClientAddresses = nil
}

// WhitelistClients adds clients (IP addresses or ranges) to the whitelist. A whitelist is mutually exclusive with a blacklist, meaning
// that all blacklisted clients are removed when calling this function.
func (mgr *ServerSecurityManager) WhitelistClients(addrs ...string) {
	mgr.deniedClientAddresses = nil

	mgr.allowedClientAddresses = append(mgr.allowedClientAddresses, addrs...)
}

// BlacklistClients adds clients to the blacklist (IP addresses or ranges) to the blacklist. A blacklist is mutually exclusive with a
// whitelist, meaning that all whitelisted clients are removed when calling this function.
func (mgr *ServerSecurityManager) BlacklistClients(addrs ...string) {
	mgr.allowedClientAddresses = nil

	mgr.deniedClientAddresses = append(mgr.deniedClientAddresses, addrs...)
}
