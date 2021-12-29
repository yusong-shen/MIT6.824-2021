package mr

import (
	"fmt"
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

func TestAskTaskRpc(t *testing.T) {
	c := Coordinator{inputfiles: []string{"file1", "file2"}}
	c.initializeTasks()
	args := AskTaskArgs{WorkerId: "2"}
	var reply AskTaskReply

	err := c.AskTask(&args, &reply)
	fmt.Println(reply.T.toString())
	assert.NoError(t, err)
	assert.Equal(t, reply.T.TaskType, 1)
	assert.Equal(t, reply.T.Inputfiles, []string{"file1"})

}

func TestInitializeTasks(t *testing.T) {
	c := Coordinator{inputfiles: []string{"file1", "file2"}}
	c.initializeTasks()
	status, ok := c.checkTaskStatus(Task{TaskType: 1})
	assert.False(t, ok)
	assert.Equal(t, status, 0)

	status, ok = c.checkTaskStatus(Task{TaskType: 1, Inputfiles: []string{"file1", "file2"}})
	assert.False(t, ok)
	assert.Equal(t, status, 0)

	status, ok = c.checkTaskStatus(Task{TaskType: 2})
	assert.False(t, ok)
	assert.Equal(t, status, 0)

	status, ok = c.checkTaskStatus(Task{TaskType: 1, Inputfiles: []string{"file1"}})
	assert.True(t, ok)
	assert.Equal(t, status, 1)

	status, ok = c.checkTaskStatus(Task{TaskType: 1, Inputfiles: []string{"file2"}})
	assert.True(t, ok)
	assert.Equal(t, status, 1)

}
