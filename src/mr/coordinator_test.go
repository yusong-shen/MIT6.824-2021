package mr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExampleRpc(t *testing.T) {
	c := Coordinator{}
	args := ExampleArgs{X: 99}
	var reply ExampleReply

	err := c.Example(&args, &reply)
	assert.NoError(t, err)
	assert.Equal(t, 100, reply.Y, "RPC reply.Y should equal to 100!")

}
