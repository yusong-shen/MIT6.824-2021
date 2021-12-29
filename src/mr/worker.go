package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

	if len(replyAskTask.T.Inputfiles) > 0 {
		inputFilename := replyAskTask.T.Inputfiles[0]
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
