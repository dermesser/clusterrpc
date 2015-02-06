package clusterrpc

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"testing"
	"time"
)

func getServer() *Server {
	srv := new(Server)
	srv.services = make(map[string]*service)

	return srv
}

func TestRegisterEndpoint(t *testing.T) {
	srv := getServer()
	f := func(cx *Context) { cx.Success([]byte("")) }

	if nil != srv.RegisterEndpoint("BogusService", "Test1", f) {
		t.Fail()
	}
}

func TestRegisterEndpointTwice(t *testing.T) {
	srv := getServer()
	f := func(cx *Context) { cx.Success([]byte("")) }

	if nil != srv.RegisterEndpoint("BogusService", "Test1", f) {
		t.Fail()
	}
	if nil == srv.RegisterEndpoint("BogusService", "Test1", f) {
		t.Fail()
	}
}

// UnregisterEndpoint should return an error when unregistering a non-existing endpoint
func TestUnregisterEndpoint(t *testing.T) {
	srv := getServer()

	if nil == srv.UnregisterEndpoint("BogusService", "Test1") {
		t.Fail()
	}
}

func TestSizebufFuncs(t *testing.T) {
	numbers := []uint64{3, 3321 << 5, 232111, 3332232, 1 << 56, 1 << 57, 1 << 63, 889991212}

	for _, v := range numbers {
		var sizebuf [8]byte = lengthToSizebuf(v)
		var back uint64 = sizebufToLength(sizebuf)

		if back != v {
			t.Fail()
		}
	}
}

func BenchmarkToSizebuf(b *testing.B) {
	numbers := []uint64{3, 3321 << 5, 232111, 3332232, 1 << 56, 1 << 57, 1 << 63, 889991212}

	for i := 0; i < b.N; i++ {
		var _ [8]byte = lengthToSizebuf(numbers[i%8])
	}
}

func BenchmarkToFromSizebuf(b *testing.B) {
	numbers := []uint64{3, 3321 << 5, 232111, 3332232, 1 << 56, 1 << 57, 1 << 63, 889991212}

	for i := 0; i < b.N; i++ {
		var sb [8]byte = lengthToSizebuf(numbers[i%8])
		var _ uint64 = sizebufToLength(sb)
	}
}

func TestLengthPrefixed(t *testing.T) {
	b := []byte{3, 23, 11, 45, 32, 11, 23, 45, 88, 99, 64, 34}

	lp := bytesToLengthPrefixed(b)

	r := bytes.NewReader(lp)

	b2, err := readSizePrefixedMessage(r)

	if err != nil || !bytes.Equal(b, b2) {
		t.Fail()
	}

}

// Tests that we can write and read records to/from files (Reader/Writer)
func TestRWPrefixed(t *testing.T) {
	f, err := ioutil.TempFile("/tmp", "clusterrpc_test_")

	if err != nil {
		t.FailNow()
	}

	defer func() { name := f.Name(); f.Close(); os.Remove(name) }()

	b1 := []byte{3, 23, 11, 45, 32, 11, 23, 45, 88, 99, 64, 34}
	b2 := []byte{3, 23, 11, 45, 32, 11, 23, 45, 88, 99, 64, 35}

	B1 := bytesToLengthPrefixed(b1)
	B2 := bytesToLengthPrefixed(b2)

	_, err1 := f.Write(B1)
	_, err2 := f.Write(B2)

	if err1 != nil || err2 != nil {
		t.FailNow()
	}

	f.Seek(0, 0)
	rb1, err1 := readSizePrefixedMessage(f)
	rb2, err2 := readSizePrefixedMessage(f)

	if err1 != nil || err2 != nil || !bytes.Equal(b1, rb1) || !bytes.Equal(b2, rb2) {
		t.Fail()
	}

}
