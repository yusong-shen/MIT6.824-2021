package mr

import "testing"

func TestRpcExample(t *testing.T) {

	got := 4 + 6
	want := 10

	if got != want {
		t.Errorf("got %q, wanted %q", got, want)
	}
}
