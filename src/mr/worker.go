package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	argsRegister := RegisterWorkerArgs{}
	replyRegister := RegisterWorkerReply{}
	CallRegisterWorker(&argsRegister, &replyRegister)

	argsAskTask := AskTaskArgs{WorkerId: string(replyRegister.WorkerId)}
	replyAskTask := AskTaskReply{}
	CallAskTask(&argsAskTask, &replyAskTask)

	task := replyAskTask.T

	// if it's a map task
	if task.TaskType == 1 {
		fmt.Println("Processing map task...")
		processMapTask(task, replyAskTask.ReduceTasksCnt, mapf)
	}

}

func CallRegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) {

	call("Coordinator.RegisterWorker", args, reply)
	fmt.Printf("CallRegisterWorker - reply.workerId: %v\n", reply.WorkerId)
}

func CallAskTask(args *AskTaskArgs, reply *AskTaskReply) {
	call("Coordinator.AskTask", args, reply)
	fmt.Printf("CallAskTask - reply.T: %v\n", reply.T)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Printf("Error: %v\n", err)
	return false
}

func ReadFile(filename string) string {
	fmt.Printf("Reading file: %v\n", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

func WriteIntermediateDataToFile(intermediateData []KeyValue, filename string) {
	file, err := json.MarshalIndent(intermediateData, "", " ")
	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}
	err = ioutil.WriteFile(filename, file, 0644)
	if err != nil {
		log.Fatalf("Error write to file: %v\n", err)
	}
}

func ReadJsonData(filename string) []KeyValue {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}
	var kvList []KeyValue
	err = json.Unmarshal(data, &kvList)
	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}
	return kvList

}

func AssignKvToReducer(intermediateData []KeyValue, intermediateDataMap map[int][]KeyValue, reduceTasksCnt int) {
	for _, kv := range intermediateData {
		iReduce := ihash(kv.Key) % reduceTasksCnt
		if _, exist := intermediateDataMap[iReduce]; !exist {
			intermediateDataMap[iReduce] = make([]KeyValue, 0)
		}
		intermediateDataMap[iReduce] = append(intermediateDataMap[iReduce], kv)
	}
}

func processMapTask(task Task, reduceTasksCnt int, mapf func(string, string) []KeyValue) {
	if len(task.Inputfiles) > 0 {
		inputFilename := task.Inputfiles[0]
		content := ReadFile(inputFilename)
		n := len(content)
		if len(content) > 10 {
			n = 10
		}
		fmt.Printf("Content: %v...\n", content[:n])
		fmt.Printf("Content len: %v\n", len(content))
		intermediateData := mapf(inputFilename, content)
		fmt.Printf("intermediateData len: %v\n", len(intermediateData))
		if len(intermediateData) > 0 {
			fmt.Printf("intermediateData[0]: %v\n", intermediateData[0])
		}

		intermediateDataMap := make(map[int][]KeyValue)
		AssignKvToReducer(intermediateData, intermediateDataMap, reduceTasksCnt)
		// intermediate files is mr-X-Y, where X is the Map task number,
		// and Y is the reduce task number.
		for iReduce, kvList := range intermediateDataMap {
			intermediateFilename := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(iReduce)
			WriteIntermediateDataToFile(kvList, intermediateFilename)

		}
	}
}
