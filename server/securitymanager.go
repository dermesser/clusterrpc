package server

import (
	"bytes"
	"errors"
	"os"

	"github.com/pebbe/zmq4"
)

const SERVER_DOMAIN = "clusterrpc.srv"

// This module manages keys for a clusterrpc server.

type serverSecurityManager struct {
	// Z85 keys
	public, private     string
	allowed_client_keys []string

	// Only set one of both!
	allowed_client_addresses []string
	denied_client_addresses  []string
}

// Set up key manager and generate new key pair.
func NewServerSecurityManager() *serverSecurityManager {
	mgr := &serverSecurityManager{}
	var err error

	mgr.public, mgr.private, err = zmq4.NewCurveKeypair()

	if err != nil {
		return nil
	}

	return mgr
}

// Apply the internal keys to the server.
// This must be called before applying Bind() on the socket!
func (mgr *serverSecurityManager) applyToServerSocket(sock *zmq4.Socket) error {
	t, err := sock.GetType()

	// Only apply to actual server sockets
	if err != nil && t != zmq4.ROUTER && t != zmq4.REP {
		return errors.New("Wrong socket type")
	}

	// start in any case (returns error if already running, ignore that)
	zmq4.AuthStart()

	if mgr.allowed_client_addresses != nil {
		// We can use a static string because this is the only server in the context
		zmq4.AuthAllow(SERVER_DOMAIN, mgr.allowed_client_addresses...)
	} else if mgr.denied_client_addresses != nil {
		zmq4.AuthDeny(SERVER_DOMAIN, mgr.denied_client_addresses...)
	}

	if mgr.allowed_client_keys != nil {
		zmq4.AuthCurveAdd(SERVER_DOMAIN, mgr.allowed_client_keys...)
	} else {
		// Make it open
		zmq4.AuthCurveAdd(SERVER_DOMAIN, zmq4.CURVE_ALLOW_ANY)
	}

	err = sock.ServerAuthCurve(SERVER_DOMAIN, mgr.private)

	if err != nil {
		return err
	}

	return nil
}

// Tear down all resources associated with authentication
func (mgr *serverSecurityManager) StopManager() {
	zmq4.AuthStop()
}

func (mgr *serverSecurityManager) SetKeys(public, private string) {
	mgr.public, mgr.private = public, private
}

func (mgr *serverSecurityManager) GetPublicKey() string {
	return mgr.public
}

func (mgr *serverSecurityManager) LoadKeys(public_file, private_file string) error {
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

func (mgr *serverSecurityManager) WriteKeys(public_file, private_file string) error {
	pubfile, err := os.OpenFile(public_file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	defer pubfile.Close()

	if err != nil {
		return err
	}

	privfile, err := os.OpenFile(private_file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	defer privfile.Close()

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

	n, err = privfile.Write([]byte(mgr.private))

	if err != nil {
		return err
	}
	if n != len(mgr.public) {
		return errors.New("Could not write correct number of bytes to private key file")
	}

	return nil
}

func (mgr *serverSecurityManager) AddClientKeys(keys ...string) {
	for _, k := range keys {
		mgr.allowed_client_keys = append(mgr.allowed_client_keys, k)
	}
}

// If this is called and no new keys are added, the server is open!
func (mgr *serverSecurityManager) ResetClientKeys() {
	mgr.allowed_client_keys = nil
}

func (mgr *serverSecurityManager) ResetBlackWhiteLists() {
	mgr.allowed_client_addresses = nil
	mgr.denied_client_addresses = nil
}

func (mgr *serverSecurityManager) WhitelistClients(addrs ...string) {
	mgr.denied_client_addresses = nil

	for _, c := range addrs {
		mgr.allowed_client_addresses = append(mgr.allowed_client_addresses, c)
	}
}

func (mgr *serverSecurityManager) BlacklistClients(addrs ...string) {
	mgr.allowed_client_addresses = nil

	for _, c := range addrs {
		mgr.denied_client_addresses = append(mgr.denied_client_addresses, c)
	}
}
