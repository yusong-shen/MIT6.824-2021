package mr

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
	c := Coordinator{inputfiles: []string{"file1", "file2"}, reduceTasksCnt: 10}
	c.initializeMapTasks()
	args := AskTaskArgs{WorkerId: "2"}
	var reply AskTaskReply

	err := c.AskTask(&args, &reply)
	fmt.Println(reply.T.toString())
	assert.NoError(t, err)
	assert.Equal(t, 1, reply.T.TaskType)
	assert.Equal(t, []string{"file1"}, reply.T.Inputfiles)
	assert.Equal(t, 10, reply.ReduceTasksCnt)

	// assert task status is 2 (in progress)
	status, ok := c.checkTaskStatus(reply.T)
	assert.True(t, ok)
	assert.Equal(t, 2, status)
}

func TestInitializeMapTasks(t *testing.T) {
	c := Coordinator{inputfiles: []string{"file1", "file2"}}
	c.initializeMapTasks()
	status, ok := c.checkTaskStatus(Task{TaskType: 1})
	assert.False(t, ok)
	assert.Equal(t, status, 0)

	status, ok = c.checkTaskStatus(Task{TaskType: 1, Inputfiles: []string{"file1", "file2"}})
	assert.False(t, ok)
	assert.Equal(t, status, 0)

	status, ok = c.checkTaskStatus(Task{TaskType: 2})
	assert.False(t, ok)
	assert.Equal(t, status, 0)

	// assert task status is 1 (idle)
	status, ok = c.checkTaskStatus(Task{TaskType: 1, Inputfiles: []string{"file1"}, TaskId: 0})
	assert.True(t, ok)
	assert.Equal(t, 1, status)

	status, ok = c.checkTaskStatus(Task{TaskType: 1, Inputfiles: []string{"file2"}, TaskId: 1})
	assert.True(t, ok)
	assert.Equal(t, 1, status)

}

func TestReportTaskStatusRpc(t *testing.T) {
	c := Coordinator{inputfiles: []string{"file1", "file2"}, reduceTasksCnt: 10}
	c.initializeMapTasks()
	assert.Equal(t, int32(2), c.getRemainingMapTasksCnt())
	assert.Equal(t, int32(-1), c.getRemainingReduceTasksCnt())

	mapTask := Task{TaskType: 1, Inputfiles: []string{"file1"}, TaskId: 0}
	arg := ReportTaskStatusArgs{T: mapTask, Status: "Completed"}
	var reply ReportTaskStatusReply
	err := c.ReportTaskStatus(&arg, &reply)

	assert.NoError(t, err)
	assert.Equal(t, int32(1), c.getRemainingMapTasksCnt())

	reduceTask := Task{TaskType: 2, Inputfiles: []string{"reduceFile"}, TaskId: 0}
	arg = ReportTaskStatusArgs{T: reduceTask, Status: "Completed"}
	reply = ReportTaskStatusReply{}
	err = c.ReportTaskStatus(&arg, &reply)
	assert.NoError(t, err)
	assert.Equal(t, int32(-2), c.getRemainingReduceTasksCnt())

}
