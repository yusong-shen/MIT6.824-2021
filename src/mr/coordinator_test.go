package mr

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAskTaskRpc(t *testing.T) {
	c := NewCoordinator([]string{"file1", "file2"}, 10)
	c.initializeMapTasks()
	args := AskTaskArgs{}
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
	assert.Equal(t, InProgress, status)
}

func TestAskTaskRpc_NotIdleMapTask(t *testing.T) {
	c := NewCoordinator([]string{"file1"}, 10)
	c.initializeMapTasks()
	args := AskTaskArgs{}
	var reply AskTaskReply

	// take the only idle map task
	err := c.AskTask(&args, &reply)
	fmt.Println(reply.T.toString())
	assert.NoError(t, err)

	// ask the task again, should get empty reply
	args = AskTaskArgs{}
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
	assert.Equal(t, Unknown, status)

	status, ok = c.checkTaskStatus(Task{TaskType: Map, Inputfiles: []string{"file1", "file2"}})
	assert.False(t, ok)
	assert.Equal(t, Unknown, status)

	status, ok = c.checkTaskStatus(Task{TaskType: Reduce})
	assert.False(t, ok)
	assert.Equal(t, Unknown, status)

	// assert task status is 1 (idle)
	status, ok = c.checkTaskStatus(Task{TaskType: Map, Inputfiles: []string{"file1"}, TaskId: 0})
	assert.True(t, ok)
	assert.Equal(t, Idle, status)

	status, ok = c.checkTaskStatus(Task{TaskType: Map, Inputfiles: []string{"file2"}, TaskId: 1})
	assert.True(t, ok)
	assert.Equal(t, Idle, status)
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

func TestAskTaskRpc_AllMapTasksComplete_ShouldGetReduceTask(t *testing.T) {
	c := NewCoordinator([]string{"file1"}, 2)
	c.initializeMapTasks()
	argsAskTask := AskTaskArgs{}
	replyAskTask := AskTaskReply{}

	// set up:
	// take the only idle map task
	err := c.AskTask(&argsAskTask, &replyAskTask)
	fmt.Println(replyAskTask.T.toString())
	assert.NoError(t, err)

	// complete the task
	filename := "mr-temp-0-0"
	os.Create("mr-temp-0-0")
	argReportTask := ReportTaskStatusArgs{T: replyAskTask.T, Status: Completed, OutputFiles: []string{filename}}
	replyReportTask := ReportTaskStatusReply{}
	err = c.ReportTaskStatus(&argReportTask, &replyReportTask)
	assert.NoError(t, err)

	// act:
	// now all the map task completed, ask task again
	argsAskTask = AskTaskArgs{}
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
	assert.Equal(t, status, InProgress)

	// clean up
	os.Remove("mr-0-0")
}

func TestDone(t *testing.T) {
	c := NewCoordinator([]string{"file1"}, 1)
	c.initializeMapTasks()
	argsAskTask := AskTaskArgs{}
	replyAskTask := AskTaskReply{}

	// set up:
	// take the only idle map task
	err := c.AskTask(&argsAskTask, &replyAskTask)
	fmt.Println(replyAskTask.T.toString())
	assert.NoError(t, err)
	assert.False(t, c.Done())

	// complete the map task
	filename := "mr-temp-0-0"
	os.Create("mr-temp-0-0")
	argReportTask := ReportTaskStatusArgs{T: replyAskTask.T, Status: Completed, OutputFiles: []string{filename}}
	replyReportTask := ReportTaskStatusReply{}
	err = c.ReportTaskStatus(&argReportTask, &replyReportTask)
	assert.NoError(t, err)
	assert.False(t, c.Done())

	// now all the map task completed, ask reduce task
	argsAskTask = AskTaskArgs{}
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

	// clean up
	os.Remove("mr-0-0")

}

func TestGetReducerId(t *testing.T) {
	tests := []struct {
		filename string
		want     int
	}{
		{filename: "mr-2-9", want: 9},
		{filename: "mr-2-9.txt", want: -1},
		{filename: "mr", want: -1},
	}

	c := NewCoordinator([]string{"file1"}, 1)

	for _, tc := range tests {
		if got := c.getReducerId(tc.filename); got != tc.want {
			t.Errorf("Input filename %v, got reducer id %v, want %v", tc.filename, got, tc.want)
		}
	}
}
