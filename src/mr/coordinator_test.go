package mr

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegisterWorkerRpc(t *testing.T) {
	c := NewCoordinator([]string{}, 0)
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
	c := NewCoordinator([]string{"file1", "file2"}, 10)
	c.initializeMapTasks()
	args := AskTaskArgs{WorkerId: "2"}
	var reply AskTaskReply

	err := c.AskTask(&args, &reply)
	fmt.Println(reply.T.toString())
	assert.NoError(t, err)
	assert.Equal(t, Map, reply.T.TaskType)
	assert.Equal(t, []string{"file1"}, reply.T.Inputfiles)
	assert.Equal(t, 10, reply.ReduceTasksCnt)

	// assert task status is 2 (in progress)
	status, ok := c.checkTaskStatus(reply.T)
	assert.True(t, ok)
	assert.Equal(t, 2, status)
}

func TestAskTaskRpc_NotIdleMapTask(t *testing.T) {
	c := NewCoordinator([]string{"file1"}, 10)
	c.initializeMapTasks()
	args := AskTaskArgs{WorkerId: "2"}
	var reply AskTaskReply

	// take the only idle map task
	err := c.AskTask(&args, &reply)
	fmt.Println(reply.T.toString())
	assert.NoError(t, err)

	// ask the task again, should get empty reply
	args = AskTaskArgs{WorkerId: "2"}
	reply = AskTaskReply{}
	err = c.AskTask(&args, &reply)
	fmt.Println(reply.T.toString())
	assert.NoError(t, err)
	assert.Equal(t, AskTaskReply{}, reply)
}

func TestInitializeMapTasks(t *testing.T) {
	c := NewCoordinator([]string{"file1", "file2"}, 0)
	c.initializeMapTasks()
	status, ok := c.checkTaskStatus(Task{TaskType: Map})
	assert.False(t, ok)
	assert.Equal(t, status, 0)

	status, ok = c.checkTaskStatus(Task{TaskType: Map, Inputfiles: []string{"file1", "file2"}})
	assert.False(t, ok)
	assert.Equal(t, status, 0)

	status, ok = c.checkTaskStatus(Task{TaskType: Reduce})
	assert.False(t, ok)
	assert.Equal(t, status, 0)

	// assert task status is 1 (idle)
	status, ok = c.checkTaskStatus(Task{TaskType: Map, Inputfiles: []string{"file1"}, TaskId: 0})
	assert.True(t, ok)
	assert.Equal(t, 1, status)

	status, ok = c.checkTaskStatus(Task{TaskType: Map, Inputfiles: []string{"file2"}, TaskId: 1})
	assert.True(t, ok)
	assert.Equal(t, 1, status)

}

func TestReportTaskStatusRpc(t *testing.T) {
	c := NewCoordinator([]string{"file1", "file2"}, 10)
	c.initializeMapTasks()
	assert.Equal(t, int32(2), c.getRemainingMapTasksCnt())
	assert.Equal(t, int32(-1), c.getRemainingReduceTasksCnt())

	mapTask := Task{TaskType: Map, Inputfiles: []string{"file1"}, TaskId: 0}
	arg := ReportTaskStatusArgs{T: mapTask, Status: Completed}
	var reply ReportTaskStatusReply
	err := c.ReportTaskStatus(&arg, &reply)

	assert.NoError(t, err)
	assert.Equal(t, int32(1), c.getRemainingMapTasksCnt())

	reduceTask := Task{TaskType: Reduce, Inputfiles: []string{"reduceFile"}, TaskId: 0}
	arg = ReportTaskStatusArgs{T: reduceTask, Status: Completed}
	reply = ReportTaskStatusReply{}
	err = c.ReportTaskStatus(&arg, &reply)
	assert.NoError(t, err)
	assert.Equal(t, int32(-2), c.getRemainingReduceTasksCnt())
}

func TestGetReducerInputFiles(t *testing.T) {
	c := NewCoordinator([]string{"file1", "file2"}, 10)
	reduceInputFiles := c.getReducerInputFiles(2)
	assert.Equal(t, []string{"mr-0-2", "mr-1-2"}, reduceInputFiles)
}

func TestInitializeReduceTasks(t *testing.T) {
	c := NewCoordinator([]string{"file1", "file2"}, 3)
	c.initializeReduceTasks()
	status, ok := c.checkTaskStatus(Task{TaskType: Reduce, Inputfiles: []string{"mr-0-0", "mr-1-0"}, TaskId: 0})
	assert.True(t, ok)
	assert.Equal(t, 1, status)

	status, ok = c.checkTaskStatus(Task{TaskType: Reduce, Inputfiles: []string{"mr-0-1", "mr-1-1"}, TaskId: 1})
	assert.True(t, ok)
	assert.Equal(t, 1, status)

	status, ok = c.checkTaskStatus(Task{TaskType: Reduce, Inputfiles: []string{"mr-0-2", "mr-1-2"}, TaskId: 2})
	assert.True(t, ok)
	assert.Equal(t, 1, status)
}

func TestAskTaskRpc_AllMapTasksComplete_ShouldGetReduceTask(t *testing.T) {
	c := NewCoordinator([]string{"file1"}, 2)
	c.initializeMapTasks()
	argsAskTask := AskTaskArgs{WorkerId: "2"}
	replyAskTask := AskTaskReply{}

	// set up:
	// take the only idle map task
	err := c.AskTask(&argsAskTask, &replyAskTask)
	fmt.Println(replyAskTask.T.toString())
	assert.NoError(t, err)

	// complete the task
	argReportTask := ReportTaskStatusArgs{T: replyAskTask.T, Status: Completed}
	replyReportTask := ReportTaskStatusReply{}
	err = c.ReportTaskStatus(&argReportTask, &replyReportTask)
	assert.NoError(t, err)

	// act:
	// now all the map task completed, ask task again
	argsAskTask = AskTaskArgs{WorkerId: "3"}
	replyAskTask = AskTaskReply{}
	err = c.AskTask(&argsAskTask, &replyAskTask)
	fmt.Println(replyAskTask.T.toString())
	assert.NoError(t, err)
	// assert worker should get reduce task
	assert.Equal(t, Reduce, replyAskTask.T.TaskType)
	assert.Equal(t, []string{"mr-0-0"}, replyAskTask.T.Inputfiles)
	assert.Equal(t, 2, replyAskTask.ReduceTasksCnt)

	// assert task status is 2 (in progress)
	status, ok := c.checkTaskStatus(replyAskTask.T)
	assert.True(t, ok)
	assert.Equal(t, 2, status)
}

func TestDone(t *testing.T) {
	c := NewCoordinator([]string{"file1"}, 1)
	c.initializeMapTasks()
	argsAskTask := AskTaskArgs{WorkerId: "2"}
	replyAskTask := AskTaskReply{}

	// set up:
	// take the only idle map task
	err := c.AskTask(&argsAskTask, &replyAskTask)
	fmt.Println(replyAskTask.T.toString())
	assert.NoError(t, err)
	assert.False(t, c.Done())

	// complete the map task
	argReportTask := ReportTaskStatusArgs{T: replyAskTask.T, Status: Completed}
	replyReportTask := ReportTaskStatusReply{}
	err = c.ReportTaskStatus(&argReportTask, &replyReportTask)
	assert.NoError(t, err)
	assert.False(t, c.Done())

	// now all the map task completed, ask reduce task
	argsAskTask = AskTaskArgs{WorkerId: "3"}
	replyAskTask = AskTaskReply{}
	err = c.AskTask(&argsAskTask, &replyAskTask)
	fmt.Println(replyAskTask.T.toString())
	assert.NoError(t, err)
	assert.False(t, c.Done())

	// complete the reduce task
	argReportTask = ReportTaskStatusArgs{T: replyAskTask.T, Status: Completed}
	replyReportTask = ReportTaskStatusReply{}
	err = c.ReportTaskStatus(&argReportTask, &replyReportTask)
	assert.NoError(t, err)

	// act:
	// assert done should return result as true
	assert.True(t, c.Done())
}
