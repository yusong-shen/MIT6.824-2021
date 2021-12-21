package main

import "testing"

func TestWcMap(t *testing.T) {

	got := 4 + 6
	want := 10

	if got != want {
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
