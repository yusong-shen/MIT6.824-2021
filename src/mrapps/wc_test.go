package main

import (
	"fmt"
	"testing"

	"6.824/mr"
)

func TestWcMap(t *testing.T) {

	got := Map("file1", "alice bob alice")
	fmt.Println(got)
	want := mr.KeyValue{Key: "alice", Value: "1"}
	if got[0] != want {
		t.Errorf("got %q, wanted %q", got, want)
	}

}

func TestWcReduce(t *testing.T) {

	got := Reduce("key1", []string{"1", "2", "4"})
	want := "3"

	if got != want {
		t.Errorf("got %q, wanted %q", got, want)
	}
}
