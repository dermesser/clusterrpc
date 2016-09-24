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
// private key and then LoadKeys() with publicFile as DONOTREAD, leaving the public key untouched)
func (mgr *keyWriteLoader) LoadKeys(publicFile, privateFile string) error {
	if publicFile != DONOTREAD {
		var err error
		mgr.public, err = readFile(publicFile)

		if err != nil {
			return err
		}
	}

	if privateFile != DONOTREAD {
		var err error
		mgr.private, err = readFile(privateFile)

		if err != nil {
			return err
		}
	}
	return nil
}

func readFile(filename string) (string, error) {
	file, err := os.Open(filename)
	defer file.Close()

	if err != nil {
		return "", err
	}

	buf := bytes.NewBuffer(nil)

	n, err := buf.ReadFrom(file)

	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", errors.New("Could not read public key")
	}

	return buf.String(), nil
}

// Writes a keypair to the supplied files.
// If one of the file names is the constant DONOTWRITE, the function will not write to that file.
// e.g. mgr.WriteKeys("pubkey.txt", server.DONOTWRITE) writes only the public key.
func (mgr *keyWriteLoader) WriteKeys(publicFile, privateFile string) error {

	if publicFile != DONOTWRITE {
		err := writeFile(publicFile, mgr.public)

		if err != nil {
			return err
		}
	}

	if privateFile != DONOTWRITE {
		err := writeFile(privateFile, mgr.private)

		if err != nil {
			return err
		}
	}
	return nil
}

func writeFile(filename, content string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	defer file.Close()

	if err != nil {
		return err
	}
	n, err := file.Write([]byte(content))

	if err != nil {
		return err
	}
	if n != 40 {
		return errors.New("Could not write correct number of bytes to public key file")
	}

	return nil
}
