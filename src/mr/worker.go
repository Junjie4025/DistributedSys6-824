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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// make rpc call to retrieve file which has not been started
	args, reply := CallMaster()

	intermediate := map[int][]KeyValue{}
	for _, filename := range reply.FileNames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		for _, kv := range kva {
			idx := ihash(kv.Key) % reply.NReduce
			intermediate[idx] = append(intermediate[idx], kv)
		}
	}

	for y, bucket := range intermediate {
		sort.Sort(ByKey(bucket))

		interFileName := fmt.Sprint("mr-", args.X, "-", y, ".json")
		file, err := os.Create(interFileName)
		if err != nil {
			log.Fatalf("cannot create a new file %s for intermidate key/value, %s", interFileName, err)
		}

		enc := json.NewEncoder(file)
		for _, kv := range bucket {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write intermidate output to json file, %s", err)
			}
		}
		file.Close()
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallMaster() (*Args, *Reply) {

	// declare an argument structure.
	args := Args{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	call("Master.Handler", &args, &reply)

	fmt.Fprintf(os.Stdout, "reply.FileNames %v\n", reply.FileNames)
	fmt.Fprintf(os.Stdout, "reply.nReduce %d\n", reply.NReduce)

	return &args, &reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
