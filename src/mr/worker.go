package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	callRegisterWorker(&argsRegister, &replyRegister)

	argsAskTask := AskTaskArgs{WorkerId: string(replyRegister.WorkerId)}
	replyAskTask := AskTaskReply{}
	callAskTask(&argsAskTask, &replyAskTask)

	task := replyAskTask.T

	// if it's a map task
	if task.TaskType == 1 {
		fmt.Println("Processing map task...")
		processMapTask(task, replyAskTask.ReduceTasksCnt, mapf)
		reportTaskComplete(task)
	} else if task.TaskType == 2 {
		fmt.Println("Processing reduce task...")

		// if it's a reduce task
		// 1. read all the intermediate data files
		mergeIntermediateData := readIntermediateFiles(task.Inputfiles)
		// 2. sort them by key
		sort.Sort(ByKey(mergeIntermediateData))
		// 3. call reducef on each distinct key in aggregated sorted intermediate data
		result := applyReducef(reducef, mergeIntermediateData)
		// 4. print the result to file mr-out-Y, where Y is the reduce task id
		outputFilename := fmt.Sprintf("mr-out-%v", task.TaskId)
		writeOutputFile(result, outputFilename)
		reportTaskComplete(task)
	}

}

func reportTaskComplete(task Task) {
	argsReportTaskStatus := ReportTaskStatusArgs{T: task, Status: "Completed"}
	replyReportTaskStatus := ReportTaskStatusReply{}
	callReportTaskStatus(&argsReportTaskStatus, &replyReportTaskStatus)
}

func callRegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) {

	call("Coordinator.RegisterWorker", args, reply)
	fmt.Printf("CallRegisterWorker - reply.workerId: %v\n", reply.WorkerId)
}

func callAskTask(args *AskTaskArgs, reply *AskTaskReply) {
	call("Coordinator.AskTask", args, reply)
	fmt.Printf("CallAskTask - reply.T: %v\n", reply.T)
}

func callReportTaskStatus(args *ReportTaskStatusArgs, reply *ReportTaskStatusReply) {
	fmt.Println("Calling ReportTaskStatus")
	call("Coordinator.ReportTaskStatus", args, reply)
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

func readFile(filename string) string {
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

func writeIntermediateDataToFile(intermediateData []KeyValue, filename string) {
	file, err := json.MarshalIndent(intermediateData, "", " ")
	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}
	err = ioutil.WriteFile(filename, file, 0644)
	if err != nil {
		log.Fatalf("Error write to file: %v\n", err)
	}
}

func readJsonData(filename string) []KeyValue {
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

func assignKvToReducer(intermediateData []KeyValue, intermediateDataMap map[int][]KeyValue, reduceTasksCnt int) {
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
		content := readFile(inputFilename)
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
		assignKvToReducer(intermediateData, intermediateDataMap, reduceTasksCnt)
		// intermediate files is mr-X-Y, where X is the Map task number,
		// and Y is the reduce task number.
		for iReduce, kvList := range intermediateDataMap {
			intermediateFilename := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(iReduce)
			writeIntermediateDataToFile(kvList, intermediateFilename)

		}
	}
}

func readIntermediateFiles(intermediateFilenames []string) []KeyValue {
	mergedData := make([]KeyValue, 0)
	for _, filename := range intermediateFilenames {
		mergedData = append(mergedData, readJsonData(filename)...)
	}
	return mergedData
}

// call Reduce on each distinct key in intermediateData
func applyReducef(reducef func(string, []string) string, intermediateData []KeyValue) []KeyValue {
	result := make([]KeyValue, 0)
	i := 0
	for i < len(intermediateData) {
		j := i + 1
		for j < len(intermediateData) && intermediateData[j].Key == intermediateData[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediateData[k].Value)
		}
		output := KeyValue{Key: intermediateData[i].Key, Value: reducef(intermediateData[i].Key, values)}
		result = append(result, output)

		i = j
	}
	return result
}

// print the result to mr-out-Y
func writeOutputFile(outputData []KeyValue, outputFilename string) {
	ofile, _ := os.Create(outputFilename)
	for _, data := range outputData {
		fmt.Fprintf(ofile, "%v %v\n", data.Key, data.Value)
	}
	ofile.Close()
}
