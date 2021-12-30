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

type RegisterWorkerArgs struct {
}

type RegisterWorkerReply struct {
	WorkerId string
}

type AskTaskArgs struct {
	WorkerId string
}

type AskTaskReply struct {
	T              Task
	ReduceTasksCnt int
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
