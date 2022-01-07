package mr

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
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
	intermediateFiles       map[int][]string
	intermediateFilesMutex  sync.Mutex
	allTasks                SafeAllTaskMap
}

type SafeStatusMap struct {
	m  map[string]TaskStatus
	mu sync.Mutex
}

type SafeAllTaskMap struct {
	m  map[string]Task
	mu sync.Mutex
}

const TaskTimeout = 10 * time.Second

// gives each input file to a map task
func (c *Coordinator) initializeMapTasks() {
	for i, file := range c.inputfiles {
		t := Task{TaskType: Map, Inputfiles: []string{file}, TaskId: i, StartTime: time.Now()}
		log.Printf("Initializing map task: %v\n", t)
		c.initTask(t)
		// if queue is full, it will block until there is some consumer complete some tasks
		c.idleMapTasksQueue <- t
	}
	log.Printf("IdleMapTaskQueue Len: %d\n", len(c.idleMapTasksQueue))

}

func (c *Coordinator) initTask(t Task) {
	tStr := t.toString()
	c.taskStatusMap.mu.Lock()
	c.taskStatusMap.m[tStr] = Idle
	c.taskStatusMap.mu.Unlock()
	// keep tracking all the tasks
	c.allTasks.mu.Lock()
	c.allTasks.m[tStr] = t
	c.allTasks.mu.Unlock()
}

func (c *Coordinator) checkTaskStatus(t Task) (TaskStatus, bool) {
	c.taskStatusMap.mu.Lock()
	defer c.taskStatusMap.mu.Unlock()
	status, ok := c.taskStatusMap.m[t.toString()]
	return status, ok
}

// RPC handlers for the worker to call.

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	log.Println("Receive AskTask RPC")
	c.scanTasks()
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
		t.StartTime = time.Now()
		log.Printf("Assign Map Task: %v\n", t)
		log.Printf("idleMapTasksQueue len: %d\n", len(c.idleMapTasksQueue))
		reply.T = t
		reply.ReduceTasksCnt = c.reduceTasksCnt
		// mark the task as in progress
		c.taskStatusMap.mu.Lock()
		c.taskStatusMap.m[t.toString()] = InProgress
		c.taskStatusMap.mu.Unlock()
		return nil
	}
	// assign reduce task when available
	if len(c.idleReduceTaskQueue) != 0 {
		t := <-c.idleReduceTaskQueue
		t.StartTime = time.Now()
		log.Printf("Assign Reduce Task: %v\n", t)
		log.Printf("idleReduceTaskQueue len: %d\n", len(c.idleReduceTaskQueue))
		reply.T = t
		reply.ReduceTasksCnt = c.reduceTasksCnt
		// mark the task as in progress
		c.taskStatusMap.mu.Lock()
		c.taskStatusMap.m[t.toString()] = InProgress
		c.taskStatusMap.mu.Unlock()
	}

	return nil
}

func (c *Coordinator) ReportTaskStatus(args *ReportTaskStatusArgs, reply *ReportTaskStatusReply) error {
	task := args.T
	if task.TaskType == Map && args.Status == Completed {
		c.taskStatusMap.mu.Lock()
		if c.taskStatusMap.m[task.toString()] != Completed {
			// only do it when task is not yet completed
			c.recordIntermediateFiles(args.OutputFiles)
			c.taskStatusMap.m[task.toString()] = Completed
		}
		c.taskStatusMap.mu.Unlock()

	} else if task.TaskType == Reduce && args.Status == Completed {
		c.taskStatusMap.mu.Lock()
		if c.taskStatusMap.m[task.toString()] != Completed {
			// only do it when task is not yet completed
			c.taskStatusMap.m[task.toString()] = Completed
		}
		c.taskStatusMap.mu.Unlock()
	}
	if c.getRemainingMapTasksCnt() == 0 && c.getRemainingReduceTasksCnt() == 0 {
		c.taskInitializationMutex.Lock()
		// check the count again to ensure reduce tasks haven't been initialized yet
		if c.getRemainingReduceTasksCnt() == 0 {
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

func (c *Coordinator) getRemainingMapTasksCnt() int {
	mcnt, _, _ := c.getTaskCnts()
	return mcnt
}

func (c *Coordinator) getRemainingReduceTasksCnt() int {
	_, rcnt, _ := c.getTaskCnts()
	return rcnt
}

func (c *Coordinator) initializeReduceTasks() {
	for i := 0; i < c.reduceTasksCnt; i++ {
		// do atomic renaming
		inputFiles := c.renameFiles(c.getReducerInputFiles(i))
		// sort the file to make sure order doesn't affect toString()
		sort.Strings(inputFiles)
		if len(inputFiles) == 0 {
			log.Printf("Skip reduce task id=%v, since there is not input file\n", i)
			continue
		}
		task := Task{TaskType: Reduce, Inputfiles: inputFiles, TaskId: i, StartTime: time.Now()}
		// update the task status map
		log.Printf("Initializing reduce task: %v\n", task)
		c.initTask(task)
		c.idleReduceTaskQueue <- task
	}
	log.Printf("IdleReduceTaskQueue Len: %d\n", len(c.idleReduceTaskQueue))

}

func (c *Coordinator) renameFiles(files []string) []string {
	ret := make([]string, 0)
	for i, file := range files {
		newName := fmt.Sprintf("mr-%v-%v", i, c.getReducerId(file))
		err := os.Rename(file, newName)
		if err != nil {
			log.Printf("Error when renaming file: %v\n", err)
		} else {
			log.Printf("Renaming file from %v to %v\n", file, newName)
			ret = append(ret, newName)
		}

	}
	return ret
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

func (c *Coordinator) scanTasks() {
	log.Println("Scanning tasks...")
	log.Printf("IdleMapTasksQueue len: %v\n", len(c.idleMapTasksQueue))
	log.Printf("IdleReduceTaskQueue len: %v\n", len(c.idleReduceTaskQueue))
	log.Printf("remainingMapTasksCnt: %v\n", c.getRemainingMapTasksCnt())
	log.Printf("remainingReduceTasksCnt: %v\n", c.getRemainingReduceTasksCnt())

	c.taskStatusMap.mu.Lock()
	defer c.taskStatusMap.mu.Unlock()
	c.allTasks.mu.Lock()
	defer c.allTasks.mu.Unlock()
	m := c.taskStatusMap.m
	for taskStr, status := range m {
		if status != InProgress {
			continue
		}
		// only check those in progress tasks
		t := c.allTasks.m[taskStr]
		if t.StartTime.Add(TaskTimeout).Before(time.Now()) {
			log.Printf("Task has been timeout: %v. Puting it back to idle queue.\n", t)
			t.StartTime = time.Now()
			c.allTasks.m[taskStr] = t
			if t.TaskType == Map {
				c.idleMapTasksQueue <- t
				m[taskStr] = Idle
				log.Printf("IdleMapTasksQueue len: %v\n", len(c.idleMapTasksQueue))
			} else if t.TaskType == Reduce {
				c.idleReduceTaskQueue <- t
				m[taskStr] = Idle
				log.Printf("IdleReduceTaskQueue len: %v\n", len(c.idleReduceTaskQueue))

			}
		}
	}
}

func (c *Coordinator) getTaskCnts() (int, int, int) {
	c.taskStatusMap.mu.Lock()
	defer c.taskStatusMap.mu.Unlock()
	mcnt, rcnt := 0, 0
	m := c.taskStatusMap.m
	for task, status := range m {
		// fmt.Println(task)
		if status == Idle || status == InProgress {
			if strings.Contains(task, "Map") {
				mcnt += 1
			}
			if strings.Contains(task, "Reduce") {
				rcnt += 1
			}
		}
	}
	return mcnt, rcnt, len(c.taskStatusMap.m)
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
	if c.getRemainingMapTasksCnt() == 0 && c.getRemainingReduceTasksCnt() == 0 {
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
	c.taskStatusMap = SafeStatusMap{m: make(map[string]TaskStatus)}
	c.idleMapTasksQueue = make(chan Task, 100)
	c.idleReduceTaskQueue = make(chan Task, 100)
	c.intermediateFiles = make(map[int][]string)
	c.allTasks = SafeAllTaskMap{m: map[string]Task{}}
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
