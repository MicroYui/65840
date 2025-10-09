package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := struct{}{}
		reply := Task{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if ok {
			switch reply.Type {
			case Wait:
				time.Sleep(time.Second)
			case Map:
				doMapTask(mapf, reply)
				reportArgs := ReportTaskArgs{ID: reply.ID, Type: Map}
				reportReply := struct{}{}
				if !call("Coordinator.ReportTaskDone", &reportArgs, &reportReply) {
					// fmt.Printf("Worker: failed to report Map task %d. Exiting.\n", reply.ID)
					return
				}
			case Reduce:
				doReduceTask(reducef, reply)
				reportArgs := ReportTaskArgs{ID: reply.ID, Type: Reduce}
				reportReply := struct{}{}
				if !call("Coordinator.ReportTaskDone", &reportArgs, &reportReply) {
					// fmt.Printf("Worker: failed to report Reduce task %d. Exiting.\n", reply.ID)
					return
				}
			case Exit:
				return
			default:
				panic("Worker error: undefined task type")
			}
		} else {
			// fmt.Println("RPC call to Coordinator.RequestTask failed. Worker exiting.")
			return
		}
	}

}

func doMapTask(mapf func(string, string) []KeyValue, task Task) {
	// fmt.Printf("Worker: Starting Map task %d\n", task.ID)

	filename := task.InputFile
	contentBytes, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	content := string(contentBytes)
	keyValues := mapf(filename, content)
	encoders := make([]*json.Encoder, task.NReduce)
	tempFiles := make([]*os.File, task.NReduce)

	for i := 0; i < task.NReduce; i++ {
		tempFile, _ := os.CreateTemp("", "mr-map-temp")
		tempFiles[i] = tempFile
		encoders[i] = json.NewEncoder(tempFile)
	}

	for _, keyValue := range keyValues {
		partition := ihash(keyValue.Key) % task.NReduce
		encoders[partition].Encode(&keyValue)
	}

	for i := 0; i < task.NReduce; i++ {
		tempName := tempFiles[i].Name()
		finalName := fmt.Sprintf("mr-%d-%d", task.ID, i)
		tempFiles[i].Close()
		os.Rename(tempName, finalName)
	}

}

func doReduceTask(reducef func(string, []string) string, task Task) {
	// fmt.Printf("Worker: Starting Reduce task %d\n", task.ID)

	kva := []KeyValue{}

	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.ID)
		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	tempFile, _ := os.CreateTemp("", "mr-reduce-temp-")

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)

		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	finalName := fmt.Sprintf("mr-out-%d", task.ID)
	tempFile.Close()
	os.Rename(tempFile.Name(), finalName)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	fmt.Println(err)
	return false
}
