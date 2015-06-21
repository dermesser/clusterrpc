package securitymanager

import (
	"bytes"
	"errors"
	"os"
)

// This struct is supposed to be embedded in the *SecurityManager types
// to enable loading and writing keypairs.
type keyWriteLoader struct {
	public, private string
}

// Loads private and public key from the specified files.
// Does not initialize a key when the file name is server.DONOTREAD (for example
// when you only want to read the private key from disk -- use SetKeys() with an empty
// private key and then LoadKeys() with public_file as DONOTREAD, leaving the public key untouched)
func (mgr *keyWriteLoader) LoadKeys(public_file, private_file string) error {
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
func (mgr *keyWriteLoader) WriteKeys(public_file, private_file string) error {

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
