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
	inputfiles      []string
	reduceTasksCnt  int
	workerCnt       int32
	workerStatusMap SafeMap
	// key: string representation of task
	// value: task status, 1-idle, 2-in progress, 3-completed
	taskStatusMap           SafeStatusMap
	idleMapTasksQueue       chan Task
	idleReduceTaskQueue     chan Task
	taskInitializationMutex sync.Mutex
	// atomic interger
	remainingMapTasksCnt    int32
	remainingReduceTasksCnt int32
}

type Task struct {
	TaskType   int // 1-map, 2-reduce
	Inputfiles []string
	TaskId     int
}

func (t Task) toString() string {
	return fmt.Sprintf("%v", t)
}

type SafeMap struct {
	m  map[string]bool
	mu sync.Mutex
}

type SafeStatusMap struct {
	m  map[string]int
	mu sync.Mutex
}

// gives each input file to a map task
func (c *Coordinator) initializeMapTasks() {
	atomic.StoreInt32(&c.remainingMapTasksCnt, int32(len(c.inputfiles)))
	for i, file := range c.inputfiles {
		t := Task{TaskType: 1, Inputfiles: []string{file}, TaskId: i}
		log.Printf("Initializing map task: %v\n", t)
		c.taskStatusMap.mu.Lock()
		c.taskStatusMap.m[t.toString()] = 1
		c.taskStatusMap.mu.Unlock()
		// if queue is full, it will block until there is some consumer complete some tasks
		c.idleMapTasksQueue <- t
	}
	log.Printf("IdleMapTaskQueue Len: %d\n", len(c.idleMapTasksQueue))

}

func (c *Coordinator) checkTaskStatus(t Task) (int, bool) {
	c.taskStatusMap.mu.Lock()
	defer c.taskStatusMap.mu.Unlock()
	status, ok := c.taskStatusMap.m[t.toString()]
	return status, ok
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {

	workerId := strconv.Itoa(int(atomic.AddInt32(&c.workerCnt, 1)))
	reply.WorkerId = workerId
	c.workerStatusMap.mu.Lock()
	c.workerStatusMap.m[workerId] = true
	c.workerStatusMap.mu.Unlock()

	return nil
}

func (c *Coordinator) checkWorkerStatus(workerId string) bool {
	c.workerStatusMap.mu.Lock()
	defer c.workerStatusMap.mu.Unlock()
	return c.workerStatusMap.m[workerId]
}

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	log.Printf("WorkerId : %v\n", args.WorkerId)
	// TODO: len(c.idleMapTasksQueue) may not be safe
	if len(c.idleMapTasksQueue) == 0 && c.getRemainingMapTasksCnt() != 0 {
		// return empty reply without any task since there is not idle map task
		// but reduce tasks haven't been available yet.
		// worker need to wait for all the map tasks to be completed
		return nil
	}
	// assign idle map task when available
	if len(c.idleMapTasksQueue) != 0 {
		t := <-c.idleMapTasksQueue
		log.Printf("Assign Map Task: %v\n", t)
		log.Printf("idleMapTasksQueue len: %d\n", len(c.idleMapTasksQueue))
		reply.T = t
		reply.ReduceTasksCnt = c.reduceTasksCnt
		// mark the task as in progress
		c.taskStatusMap.mu.Lock()
		c.taskStatusMap.m[t.toString()] = 2
		c.taskStatusMap.mu.Unlock()
		return nil
	}
	// assign reduce task when available
	if len(c.idleReduceTaskQueue) != 0 {
		t := <-c.idleReduceTaskQueue
		log.Printf("Assign Reduce Task: %v\n", t)
		log.Printf("idleReduceTaskQueue len: %d\n", len(c.idleReduceTaskQueue))
		reply.T = t
		reply.ReduceTasksCnt = c.reduceTasksCnt
		// mark the task as in progress
		c.taskStatusMap.mu.Lock()
		c.taskStatusMap.m[t.toString()] = 2
		c.taskStatusMap.mu.Unlock()
	}

	return nil
}

func (c *Coordinator) ReportTaskStatus(args *ReportTaskStatusArgs, reply *ReportTaskStatusReply) error {
	task := args.T
	if task.TaskType == 1 && args.Status == "Completed" {
		atomic.AddInt32(&c.remainingMapTasksCnt, -1)
	} else if task.TaskType == 2 && args.Status == "Completed" {
		atomic.AddInt32(&c.remainingReduceTasksCnt, -1)
	}
	// mark the task as completed
	c.taskStatusMap.mu.Lock()
	c.taskStatusMap.m[task.toString()] = 3
	c.taskStatusMap.mu.Unlock()
	if c.getRemainingMapTasksCnt() == 0 && c.getRemainingReduceTasksCnt() == -1 {
		c.taskInitializationMutex.Lock()
		// check the count again to ensure reduce tasks haven't been initialized yet
		if c.getRemainingReduceTasksCnt() == -1 {
			c.initializeReduceTasks()
		}
		c.taskInitializationMutex.Unlock()
	}
	return nil
}

func (c *Coordinator) getRemainingMapTasksCnt() int32 {
	return atomic.LoadInt32(&c.remainingMapTasksCnt)
}

func (c *Coordinator) getRemainingReduceTasksCnt() int32 {
	return atomic.LoadInt32(&c.remainingReduceTasksCnt)
}

func (c *Coordinator) initializeReduceTasks() {
	atomic.StoreInt32(&c.remainingReduceTasksCnt, int32(c.reduceTasksCnt))
	for i := 0; i < c.reduceTasksCnt; i++ {
		task := Task{TaskType: 2, Inputfiles: c.getReducerInputFiles(i), TaskId: i}
		// update the task status map
		log.Printf("Initializing reduce task: %v\n", task)
		c.taskStatusMap.mu.Lock()
		c.taskStatusMap.m[task.toString()] = 1
		c.taskStatusMap.mu.Unlock()
		c.idleReduceTaskQueue <- task
	}
	log.Printf("IdleReduceTaskQueue Len: %d\n", len(c.idleReduceTaskQueue))

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
	if atomic.LoadInt32(&c.remainingMapTasksCnt) == 0 && atomic.LoadInt32(&c.remainingReduceTasksCnt) == 0 {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func NewCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{inputfiles: files, reduceTasksCnt: nReduce}
	c.workerStatusMap = SafeMap{m: make(map[string]bool)}
	c.taskStatusMap = SafeStatusMap{m: make(map[string]int)}
	c.idleMapTasksQueue = make(chan Task, 100)
	c.idleReduceTaskQueue = make(chan Task, 100)
	c.remainingReduceTasksCnt = -1
	return &c
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := NewCoordinator(files, nReduce)
	c.initializeMapTasks()

	c.server()
	return c
}
