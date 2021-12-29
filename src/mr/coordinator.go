package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"sync/atomic"
)

type Coordinator struct {
	inputfiles     []string
	reduceTasksCnt int
}

type SafeStatusMap struct {
	m  map[string]int
	mu sync.Mutex
}

// key: string representation of task
// value: task status, 1-idle, 2-in progress, 3-completed
var taskStatusMap SafeStatusMap = SafeStatusMap{m: make(map[string]int)}
var idleTasksQueue chan Task = make(chan Task, 100)

func (c *Coordinator) initializeTasks() {

	for _, file := range c.inputfiles {
		t := Task{TaskType: 1, Inputfiles: []string{file}}
		fmt.Printf("Initializing task: %v\n", t)
		taskStatusMap.mu.Lock()
		taskStatusMap.m[t.toString()] = 1
		taskStatusMap.mu.Unlock()
		// TODO: what if queue is full?
		idleTasksQueue <- t
	}
	fmt.Printf("Len: %d\n", len(idleTasksQueue))

}

func (c *Coordinator) checkTaskStatus(t Task) (int, bool) {
	taskStatusMap.mu.Lock()
	defer taskStatusMap.mu.Unlock()
	status, ok := taskStatusMap.m[t.toString()]
	return status, ok
}

// Your code here -- RPC handlers for the worker to call.
type Task struct {
	TaskType   int // 0-map, 1-reduce
	Inputfiles []string
}

func (t Task) toString() string {
	return fmt.Sprintf("%v", t)
}

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

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	fmt.Printf("WorkerId : %v\n", args.WorkerId)
	t := <-idleTasksQueue
	fmt.Printf("Len: %d\n", len(idleTasksQueue))
	fmt.Printf("Task: %v\n", t)
	reply.T = t
	return nil
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
	c := Coordinator{inputfiles: files, reduceTasksCnt: nReduce}
	c.initializeTasks()
	// TODO: Your code here.

	c.server()
	return &c
}
