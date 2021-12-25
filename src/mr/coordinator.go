package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"sync/atomic"
)

type Coordinator struct {
	// TODO: Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Add your RPC definitions here.
var workerCnt int32

type SafeMap struct {
	m  map[string]bool
	mu sync.Mutex
}

var workerStatusMap SafeMap = SafeMap{m: make(map[string]bool)}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {

	workerId := strconv.Itoa(int(atomic.AddInt32(&workerCnt, 1)))
	reply.WorkerId = workerId
	workerStatusMap.mu.Lock()
	workerStatusMap.m[workerId] = true
	workerStatusMap.mu.Unlock()

	return nil
}

func (c *Coordinator) checkWorkerStatus(workerId string) bool {
	workerStatusMap.mu.Lock()
	defer workerStatusMap.mu.Unlock()
	return workerStatusMap.m[workerId]
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// TODO: Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// TODO: Your code here.

	c.server()
	return &c
}
