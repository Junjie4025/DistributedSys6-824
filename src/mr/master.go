package mr

import (
	"fmt"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	TasksNotStarted []string
	TasksNotCompleted map[string]bool
	NReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Handler(args *Args, reply *Reply) error {
	if len(m.TasksNotStarted) > 0 {
		fileName := m.TasksNotStarted[0]
		m.TasksNotStarted = m.TasksNotStarted[1:]
		reply.FileNames = append(reply.FileNames, fileName)
		reply.NReduce = m.NReduce
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{make([]string, 0), make(map[string]bool), nReduce}

	fmt.Fprintf(os.Stdout, "nReduce: %d\n", m.NReduce)

	for _, fileName := range files {
		m.TasksNotStarted = append(m.TasksNotStarted, fileName)
	}

	fmt.Fprintf(os.Stdout, "%v\n", m.TasksNotStarted[0:])

	m.server()
	return &m
}
