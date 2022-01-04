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

type Task struct {
	TaskType   int // 1-map, 2-reduce
	Inputfiles []string
	TaskId     int
}

func (t Task) toString() string {
	return fmt.Sprintf("%v", t)
}

var workerCnt int32

type SafeMap struct {
	m  map[string]bool
	mu sync.Mutex
}

var workerStatusMap SafeMap = SafeMap{m: make(map[string]bool)}

type SafeStatusMap struct {
	m  map[string]int
	mu sync.Mutex
}

// key: string representation of task
// value: task status, 1-idle, 2-in progress, 3-completed
var taskStatusMap SafeStatusMap = SafeStatusMap{m: make(map[string]int)}
var idleMapTasksQueue chan Task = make(chan Task, 100)
var idleReduceTaskQueue chan Task = make(chan Task, 100)
var taskInitializationMutex sync.Mutex

// atomic interger
var remainingMapTasksCnt int32
var remainingReduceTasksCnt int32

// gives each input file to a map task
func (c *Coordinator) initializeMapTasks() {
	atomic.StoreInt32(&remainingMapTasksCnt, int32(len(c.inputfiles)))
	atomic.StoreInt32(&remainingReduceTasksCnt, -1)
	for i, file := range c.inputfiles {
		t := Task{TaskType: 1, Inputfiles: []string{file}, TaskId: i}
		fmt.Printf("Initializing map task: %v\n", t)
		taskStatusMap.mu.Lock()
		taskStatusMap.m[t.toString()] = 1
		taskStatusMap.mu.Unlock()
		// if queue is full, it will block until there is some consumer complete some tasks
		idleMapTasksQueue <- t
	}
	fmt.Printf("IdleMapTaskQueue Len: %d\n", len(idleMapTasksQueue))

}

func (c *Coordinator) checkTaskStatus(t Task) (int, bool) {
	taskStatusMap.mu.Lock()
	defer taskStatusMap.mu.Unlock()
	status, ok := taskStatusMap.m[t.toString()]
	return status, ok
}

// Your code here -- RPC handlers for the worker to call.

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

	if len(idleMapTasksQueue) == 0 && c.getRemainingMapTasksCnt() != 0 {
		// return empty reply without any task since there is not idle map task
		// but reduce tasks haven't been available yet.
		// worker need to wait for all the map tasks to be completed
		return nil
	}
	// assign idle map task when available
	if len(idleMapTasksQueue) != 0 {
		t := <-idleMapTasksQueue
		fmt.Printf("Assign Map Task: %v\n", t)
		fmt.Printf("idleMapTasksQueue len: %d\n", len(idleMapTasksQueue))
		reply.T = t
		reply.ReduceTasksCnt = c.reduceTasksCnt
		// mark the task as in progress
		taskStatusMap.mu.Lock()
		taskStatusMap.m[t.toString()] = 2
		taskStatusMap.mu.Unlock()
		return nil
	}
	// assign reduce task when available
	if len(idleReduceTaskQueue) != 0 {
		t := <-idleReduceTaskQueue
		fmt.Printf("Assign Reduce Task: %v\n", t)
		fmt.Printf("idleReduceTaskQueue len: %d\n", len(idleReduceTaskQueue))
		reply.T = t
		reply.ReduceTasksCnt = c.reduceTasksCnt
		// mark the task as in progress
		taskStatusMap.mu.Lock()
		taskStatusMap.m[t.toString()] = 2
		taskStatusMap.mu.Unlock()
	}

	return nil
}

func (c *Coordinator) ReportTaskStatus(args *ReportTaskStatusArgs, reply *ReportTaskStatusReply) error {
	task := args.T
	if task.TaskType == 1 && args.Status == "Completed" {
		atomic.AddInt32(&remainingMapTasksCnt, -1)
	} else if task.TaskType == 2 && args.Status == "Completed" {
		atomic.AddInt32(&remainingReduceTasksCnt, -1)
	}
	// mark the task as completed
	taskStatusMap.mu.Lock()
	taskStatusMap.m[task.toString()] = 3
	taskStatusMap.mu.Unlock()
	if c.getRemainingMapTasksCnt() == 0 && c.getRemainingReduceTasksCnt() == -1 {
		taskInitializationMutex.Lock()
		// check the count again to ensure reduce tasks haven't been initialized yet
		if c.getRemainingReduceTasksCnt() == -1 {
			c.initializeReduceTasks()
		}
		taskInitializationMutex.Unlock()
	}
	return nil
}

func (c *Coordinator) getRemainingMapTasksCnt() int32 {
	return atomic.LoadInt32(&remainingMapTasksCnt)
}

func (c *Coordinator) getRemainingReduceTasksCnt() int32 {
	return atomic.LoadInt32(&remainingReduceTasksCnt)
}

func (c *Coordinator) initializeReduceTasks() {
	atomic.StoreInt32(&remainingReduceTasksCnt, int32(c.reduceTasksCnt))
	for i := 0; i < c.reduceTasksCnt; i++ {
		task := Task{TaskType: 2, Inputfiles: c.getReducerInputFiles(i), TaskId: i}
		// update the task status map
		fmt.Printf("Initializing reduce task: %v\n", task)
		taskStatusMap.mu.Lock()
		taskStatusMap.m[task.toString()] = 1
		taskStatusMap.mu.Unlock()
		idleReduceTaskQueue <- task
	}
	fmt.Printf("IdleReduceTaskQueue Len: %d\n", len(idleReduceTaskQueue))

}

func (c *Coordinator) getReducerInputFiles(reduceTaskId int) []string {
	inputFiles := make([]string, 0)
	// file name looks like mr-X-Y, where X is the Map task number,
	// and Y is the reduce task number
	for i := 0; i < len(c.inputfiles); i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, reduceTaskId)
		inputFiles = append(inputFiles, filename)
	}
	return inputFiles
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
	if atomic.LoadInt32(&remainingMapTasksCnt) == 0 && atomic.LoadInt32(&remainingReduceTasksCnt) == 0 {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{inputfiles: files, reduceTasksCnt: nReduce}
	c.initializeMapTasks()

	c.server()
	return &c
}
