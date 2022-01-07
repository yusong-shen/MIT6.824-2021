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
	"time"
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

	for {
		ok := askAndProcessTask(mapf, reducef)
		if !ok {
			log.Println("Something is wrong when contacting the coordinator, exit.")
			break
		}
		// sleep for 0.5s and ask again
		log.Println("sleep for 0.5s and ask again.")
		time.Sleep(500 * time.Millisecond)
	}

}

func askAndProcessTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {
	argsAskTask := AskTaskArgs{}
	replyAskTask := AskTaskReply{}
	ok := callAskTask(&argsAskTask, &replyAskTask)
	if !ok {
		return false
	}
	task := replyAskTask.T

	// if it's a map task
	if task.TaskType == 1 {
		log.Println("Processing map task...")
		filenames := processMapTask(task, replyAskTask.ReduceTasksCnt, mapf)
		reportTaskComplete(task, filenames)
	} else if task.TaskType == 2 {
		log.Println("Processing reduce task...")
		// if it's a reduce task
		filename := processReduceTask(task, reducef)
		reportTaskComplete(task, []string{filename})
	}
	return true
}

func reportTaskComplete(task Task, filenames []string) bool {
	argsReportTaskStatus := ReportTaskStatusArgs{T: task, Status: Completed, OutputFiles: filenames}
	replyReportTaskStatus := ReportTaskStatusReply{}
	return callReportTaskStatus(&argsReportTaskStatus, &replyReportTaskStatus)
}

func callAskTask(args *AskTaskArgs, reply *AskTaskReply) bool {
	ok := call("Coordinator.AskTask", args, reply)
	log.Printf("CallAskTask - reply.T: %v\n", reply.T)
	return ok
}

func callReportTaskStatus(args *ReportTaskStatusArgs, reply *ReportTaskStatusReply) bool {
	log.Println("Calling ReportTaskStatus")
	ok := call("Coordinator.ReportTaskStatus", args, reply)
	return ok
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Printf("Error: %v\n", err)
	return false
}

func readFile(filename string) (string, error) {
	log.Printf("Reading file: %v\n", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Error: cannot open %v\n", filename)
		return "", err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("Error: cannot read %v\n", filename)
		return "", err
	}
	file.Close()
	return string(content), nil
}

func writeIntermediateDataToFile(intermediateData []KeyValue, iReduce int) string {
	data, err := json.MarshalIndent(intermediateData, "", " ")
	if err != nil {
		log.Printf("Error: %v\n", err)
	}
	// use default os temp directory
	filenamePattern := fmt.Sprintf("mr-temp-*-%v", iReduce)
	tempFile, err := ioutil.TempFile("", filenamePattern)
	if err != nil {
		log.Printf("Error when creating temp file: %v\n", err)
	}
	filename := tempFile.Name()
	log.Printf("Create temp file: %v\n", filename)
	_, err = tempFile.Write(data)
	if err != nil {
		log.Printf("Error write to file: %v\n", err)
	}
	return filename
}

func readJsonData(filename string) ([]KeyValue, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Printf("Error: %v\n", err)
		return nil, err
	}
	var kvList []KeyValue
	err = json.Unmarshal(data, &kvList)
	if err != nil {
		log.Printf("Error: %v\n", err)
		return nil, err
	}
	return kvList, nil

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

func processMapTask(task Task, reduceTasksCnt int, mapf func(string, string) []KeyValue) []string {
	intermediateFilenames := make([]string, 0)
	if len(task.Inputfiles) > 0 {
		inputFilename := task.Inputfiles[0]
		content, err := readFile(inputFilename)
		if err != nil {
			log.Print("processMapTask - reading input file error", err)
		}
		log.Printf("Content len: %v\n", len(content))
		intermediateData := mapf(inputFilename, content)
		log.Printf("intermediateData len: %v\n", len(intermediateData))
		if len(intermediateData) > 0 {
			log.Printf("intermediateData[0]: %v\n", intermediateData[0])
		}

		intermediateDataMap := make(map[int][]KeyValue)
		assignKvToReducer(intermediateData, intermediateDataMap, reduceTasksCnt)
		// intermediate files is temp-Y, where Y is the reduce task number.
		for iReduce, kvList := range intermediateDataMap {
			filename := writeIntermediateDataToFile(kvList, iReduce)
			intermediateFilenames = append(intermediateFilenames, filename)

		}
	}
	return intermediateFilenames
}

func readIntermediateFiles(intermediateFilenames []string) []KeyValue {
	mergedData := make([]KeyValue, 0)
	for _, filename := range intermediateFilenames {
		data, err := readJsonData(filename)
		if err == nil {
			mergedData = append(mergedData, data...)
		} else {
			log.Printf("Skip reading file: %v", filename)
		}
	}
	return mergedData
}

// call Reduce on each distinct key in intermediateData
// and print the result to mr-out-Y
func applyReducefAndWriteOutputfile(reducef func(string, []string) string, intermediateData []KeyValue, outputFilename string) {
	i := 0
	ofile, _ := os.Create(outputFilename)
	defer ofile.Close()
	for i < len(intermediateData) {
		j := i + 1
		for j < len(intermediateData) && intermediateData[j].Key == intermediateData[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediateData[k].Value)
		}
		value := reducef(intermediateData[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediateData[i].Key, value)

		i = j
	}
}

func processReduceTask(task Task, reducef func(string, []string) string) string {
	// 1. read all the intermediate data files
	mergeIntermediateData := readIntermediateFiles(task.Inputfiles)
	// 2. sort them by key
	sort.Sort(ByKey(mergeIntermediateData))
	// 3. call reducef on each distinct key in aggregated sorted intermediate data
	// 4. print the result to file mr-out-Y, where Y is the reduce task id
	outputFilename := fmt.Sprintf("mr-out-%v", task.TaskId)
	applyReducefAndWriteOutputfile(reducef, mergeIntermediateData, outputFilename)
	return outputFilename
}
