package securitymanager

import (
	"os"
	"testing"
)

func TestWriteLoad(t *testing.T) {
	mgr := NewClientSecurityManager()

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
