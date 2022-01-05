package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type AskTaskArgs struct {
}

type AskTaskReply struct {
	T              Task
	ReduceTasksCnt int
}

// TODO: change status from string to enum
type ReportTaskStatusArgs struct {
	T           Task
	Status      TaskStatus
	OutputFiles []string
}

type ReportTaskStatusReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
