package securitymanager

import (
	"os"
	"testing"
)

func TestWriteLoadServer(t *testing.T) {
	mgr := NewServerSecurityManager()

	err := mgr.WriteKeys("pubkey.txt", "privkey.txt")

	if err != nil {
		t.Error(err.Error())
		return
	}

	err = mgr.LoadKeys("pubkey.txt", "privkey.txt")

	if err != nil {
		t.Error(err.Error())
		return
	}

	os.Remove("privkey.txt")
	os.Remove("pubkey.txt")
}

func TestKeyMgmt(t *testing.T) {
	mgr := NewServerSecurityManager()

	mgr.AddClientKeys("a", "b", "c")

	if mgr.allowed_client_keys == nil || len(mgr.allowed_client_keys) != 3 {
		t.Error("List of client keys is incorrect")
		return
	}

	mgr.ResetClientKeys()

	if mgr.allowed_client_keys != nil {
		t.Error("resetClientKeys() does not work.")
	}
}

func TestListingExclusive(t *testing.T) {
	mgr := NewServerSecurityManager()

	mgr.WhitelistClients("a", "b", "c")

	if mgr.allowed_client_addresses == nil || len(mgr.allowed_client_addresses) != 3 {
		t.Error("Whitelist of clients is not correct.")
		return
	}

	mgr.BlacklistClients("d", "e", "f")

	if mgr.allowed_client_addresses != nil {
		t.Error("Whitelist was not reset")
	}
	if mgr.denied_client_addresses == nil || len(mgr.denied_client_addresses) != 3 {
		t.Error("Blacklist of clients is not correct")
		return
	}
}

func TestExplicitKeys(t *testing.T) {
	mgr := NewServerSecurityManager()

	mgr.SetKeys("pub", "priv")

	if mgr.GetPublicKey() != "pub" {
		t.Error("Wrong public key returned")
	}

	if mgr.public != "pub" || mgr.private != "priv" {
		t.Error("Wrong internal keys")
	}
}
