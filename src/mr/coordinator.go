package mr

import (
	"os"
	"strconv"
	"strings"

	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
)

type Coordinator struct {
	inputfiles     []string
	reduceTasksCnt int
	// key: string representation of task
	// value: task status, 1-idle, 2-in progress, 3-completed
	taskStatusMap           SafeStatusMap
	idleMapTasksQueue       chan Task
	idleReduceTaskQueue     chan Task
	taskInitializationMutex sync.Mutex
	// atomic interger
	remainingMapTasksCnt    int32
	remainingReduceTasksCnt int32
	intermediateFiles       map[int][]string
	intermediateFilesMutex  sync.Mutex
}

type SafeStatusMap struct {
	m  map[string]int
	mu sync.Mutex
}

// gives each input file to a map task
func (c *Coordinator) initializeMapTasks() {
	atomic.StoreInt32(&c.remainingMapTasksCnt, int32(len(c.inputfiles)))
	for i, file := range c.inputfiles {
		t := Task{TaskType: Map, Inputfiles: []string{file}, TaskId: i}
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

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	log.Println("Receive AskTask RPC")
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
	if task.TaskType == Map && args.Status == Completed {
		atomic.AddInt32(&c.remainingMapTasksCnt, -1)
		c.recordIntermediateFiles(args.OutputFiles)
	} else if task.TaskType == Reduce && args.Status == Completed {
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

func (c *Coordinator) recordIntermediateFiles(filenames []string) {
	c.intermediateFilesMutex.Lock()
	defer c.intermediateFilesMutex.Unlock()
	for _, file := range filenames {
		id := c.getReducerId(file)
		_, exist := c.intermediateFiles[id]
		if !exist {
			c.intermediateFiles[id] = make([]string, 0)
		}
		c.intermediateFiles[id] = append(c.intermediateFiles[id], file)
	}
}

func (c *Coordinator) getReducerId(filename string) int {
	lastDash := strings.LastIndex(filename, "-")
	if lastDash == -1 {
		return -1
	}
	idStr := filename[lastDash+1:]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return -1
	}
	return id
}

func (c *Coordinator) getRemainingMapTasksCnt() int32 {
	return atomic.LoadInt32(&c.remainingMapTasksCnt)
}

func (c *Coordinator) getRemainingReduceTasksCnt() int32 {
	return atomic.LoadInt32(&c.remainingReduceTasksCnt)
}

func (c *Coordinator) initializeReduceTasks() {
	atomic.StoreInt32(&c.remainingReduceTasksCnt, 0)
	for i := 0; i < c.reduceTasksCnt; i++ {
		inputFiles := c.getReducerInputFiles(i)
		if len(inputFiles) == 0 {
			log.Printf("Skip reduce task id=%v, since there is not input file\n", i)
			continue
		}
		task := Task{TaskType: Reduce, Inputfiles: c.getReducerInputFiles(i), TaskId: i}
		// update the task status map
		log.Printf("Initializing reduce task: %v\n", task)
		c.taskStatusMap.mu.Lock()
		c.taskStatusMap.m[task.toString()] = 1
		c.taskStatusMap.mu.Unlock()
		c.idleReduceTaskQueue <- task
		atomic.AddInt32(&c.remainingReduceTasksCnt, 1)
	}
	log.Printf("IdleReduceTaskQueue Len: %d\n", len(c.idleReduceTaskQueue))

}

func (c *Coordinator) getReducerInputFiles(reduceTaskId int) []string {
	c.intermediateFilesMutex.Lock()
	defer c.intermediateFilesMutex.Unlock()
	val, exist := c.intermediateFiles[reduceTaskId]
	if !exist {
		return make([]string, 0)
	}
	return val
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
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
	c.taskStatusMap = SafeStatusMap{m: make(map[string]int)}
	c.idleMapTasksQueue = make(chan Task, 100)
	c.idleReduceTaskQueue = make(chan Task, 100)
	c.remainingReduceTasksCnt = -1
	c.intermediateFiles = make(map[int][]string)
	return &c
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// // disable log
	// log.SetOutput(ioutil.Discard)
	// log.SetFlags(0)
	c := NewCoordinator(files, nReduce)
	c.initializeMapTasks()

	c.server()
	return c
}
