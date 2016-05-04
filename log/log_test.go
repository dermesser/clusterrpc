package log

import "testing"

func TestRandomStringIsRandom(t *testing.T) {
	a := GetLogToken()
	b := GetLogToken()
	if a == b {
		t.Fatal("strings are equal:", a, b)
	}
}
