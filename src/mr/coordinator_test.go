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

func TestRegisterWorkerRpc(t *testing.T) {
	c := Coordinator{}
	args := RegisterWorkerArgs{}
	var reply RegisterWorkerReply

	assert.False(t, c.checkWorkerStatus("1"))
	assert.False(t, c.checkWorkerStatus("2"))

	// register 1st worker
	err := c.RegisterWorker(&args, &reply)
	assert.NoError(t, err)
	assert.Equal(t, "1", reply.WorkerId)
	c.checkWorkerStatus("1")
	assert.True(t, c.checkWorkerStatus("1"))

	// register 2nd worker
	args = RegisterWorkerArgs{}
	reply = RegisterWorkerReply{}
	err = c.RegisterWorker(&args, &reply)
	assert.NoError(t, err)
	assert.Equal(t, "2", reply.WorkerId)
	assert.True(t, c.checkWorkerStatus("2"))

}
