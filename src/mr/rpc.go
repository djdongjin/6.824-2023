package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// MRRequest is a request from worker to coordinator requesting a task (map or reduce).
// It's empty because the coordinator doesn't need any information from the worker.
type MRRequest struct{}

// MRReply is a reply from coordinator to worker containing a task (map or reduce)
// related information.
type MRReply struct {
	JobType  string // map, reduce, wait, end
	JobID    int
	InpFiles []string // for map, it has only one file
	OutpFile string   // for map, mr-x-; for reduce, mr-out-y
	NReduce  int      // for map, it's the number of reduce tasks
}

// Possible job names
const (
	JobMap    = "map"
	JobReduce = "reduce"
	JobWait   = "wait"
	JobEnd    = "end"
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
