package server

import (
	"bytes"
	"errors"
	"os"

	"github.com/pebbe/zmq4"
)

const DONOTWRITE = "___donotwrite_key_to_file"
const DONOTREAD = "___donotread_key_from_file"
const SERVER_DOMAIN = "clusterrpc.srv"

// This module manages keys for a clusterrpc server. It is built after the API calls
// as shown in the Iron House example of ZeroMQs CURVE security documentation.

// A ServerSecurityManager can be supplied to NewServer(). It then sets up encryption and
// authentication.
// The security manager is very easy to use and enables both cryptographic/CURVE security and authentication
// and (additionally - on top of that) IP authentication.
type ServerSecurityManager struct {
	// Z85 keys
	public, private     string
	allowed_client_keys []string

	// Only set one of both!
	allowed_client_addresses []string
	denied_client_addresses  []string
}

// Set up key manager and generate new key pair.
func NewServerSecurityManager() *ServerSecurityManager {
	mgr := &ServerSecurityManager{}
	var err error

	mgr.public, mgr.private, err = zmq4.NewCurveKeypair()

	if err != nil {
		return nil
	}

	return mgr
}

// Apply the internal keys to the server.
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
	if err == nil && t != zmq4.ROUTER && t != zmq4.REP {
		return errors.New("Wrong socket type (not ROUTER, REP)")
	} else if err != nil {
		return err
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
func (mgr *ServerSecurityManager) StopManager() {
	zmq4.AuthStop()
}

// Set the public and private keys of the server.
func (mgr *ServerSecurityManager) SetKeys(public, private string) {
	mgr.public, mgr.private = public, private
}

// Returns the public key of the server.
func (mgr *ServerSecurityManager) GetPublicKey() string {
	return mgr.public
}

// Loads private and public key from the specified files.
// Does not initialize a key when the file name is server.DONOTREAD (for example
// when you only want to read the private key from disk -- use SetKeys() with an empty
// private key and then LoadKeys() with public_file as DONOTREAD, leaving the public key untouched)
func (mgr *ServerSecurityManager) LoadKeys(public_file, private_file string) error {
	if public_file != DONOTREAD {
		pubfile, err := os.Open(public_file)
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
		mgr.public = pubkeybuf.String()
	}

	if private_file != DONOTREAD {
		privfile, err := os.Open(private_file)
		defer privfile.Close()

		if err != nil {
			return err
		}

		privkeybuf := bytes.NewBuffer(nil)
		n, err := privkeybuf.ReadFrom(privfile)

		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("Could not read private key")
		}

		mgr.private = privkeybuf.String()

	}
	return nil
}

// Writes a keypair to the supplied files.
// If one of the file names is the constant DONOTWRITE, the function will not write to that file.
// e.g. mgr.WriteKeys("pubkey.txt", server.DONOTWRITE) writes only the public key.
func (mgr *ServerSecurityManager) WriteKeys(public_file, private_file string) error {

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

// Add keys of clients that are accepted.
func (mgr *ServerSecurityManager) AddClientKeys(keys ...string) {
	for _, k := range keys {
		mgr.allowed_client_keys = append(mgr.allowed_client_keys, k)
	}
}

// Remove all clients from the whitelist, effectively enforcing an OPEN policy
func (mgr *ServerSecurityManager) ResetClientKeys() {
	mgr.allowed_client_keys = nil
}

// Remove all clients from the blacklist, effectively enforcing an OPEN policy
func (mgr *ServerSecurityManager) ResetBlackWhiteLists() {
	mgr.allowed_client_addresses = nil
	mgr.denied_client_addresses = nil
}

// Add clients (IP addresses or ranges) to the whitelist. A whitelist is mutually exclusive with a blacklist, meaning
// that all blacklisted clients are removed when calling this function.
func (mgr *ServerSecurityManager) WhitelistClients(addrs ...string) {
	mgr.denied_client_addresses = nil

	for _, c := range addrs {
		mgr.allowed_client_addresses = append(mgr.allowed_client_addresses, c)
	}
}

// Add clients to the blacklist (IP addresses or ranges) to the blacklist. A blacklist is mutually exclusive with a
// whitelist, meaning that all whitelisted clients are removed when calling this function.
func (mgr *ServerSecurityManager) BlacklistClients(addrs ...string) {
	mgr.allowed_client_addresses = nil

	for _, c := range addrs {
		mgr.denied_client_addresses = append(mgr.denied_client_addresses, c)
	}
}
