package store

import (
	"bytes"
	"os"
	"testing"
)

func TestMain(t *testing.T) {
	file, _ := os.Open("instructions.txt")
	defer file.Close()
	out := bytes.NewBuffer([]byte{})

	store := Initialize()
	store.Stdin = file
	store.Stdout = out
	store.Start()
	expected := "1ajaynil"
	actual := out.String()
	if expected != actual {
		t.Logf("exepectd the out put to be %s. but got %s", expected, actual)
		t.Fail()
	}
}
