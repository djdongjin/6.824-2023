package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// These fields are input parameters and shouldn't be modified, so their
	// access doesn't require mutex.
	mapInputFiles []string
	nMap          int // = len(mapInputFiles)
	nReduce       int

	mu sync.Mutex
	// map/reduce job ids that can be assigned.
	mapJobs    []int
	reduceJobs []int
	// count of completed map/reduce jobs; use map instead an integer count to
	// avoid counting duplicated completion.
	mapCompleted    map[int]struct{}
	reduceCompleted map[int]struct{}
}

// Your code here -- RPC handlers for the worker to call.

// AskJob handles a worker request for a map/reduce job. The workflow of a map-reduce
// job should have the following stages:
// 1. (JobMap) Assign a map job to a worker;
// 2. (JobWait) Wait all map jobs to be completed;
// 3. (JobReduce) After all map jobs are completed, assign a reduce job to a worker;
// 4. (JobWait) Wait all reduce jobs to be completed;
// 5. (JobEnd) After all reduce jobs are completed, the job is done.
//
// 2 and 4 can be merged into a single condition check, but to align with the
// workflow, I duplicate the logic for easy understanding.
func (c *Coordinator) AskJob(req *MRRequest, reply *MRReply) error {
	log.Println("[Coordinator]rpc requeste received")

	c.mu.Lock()
	defer c.mu.Unlock()

	switch {
	// 1. JobMap
	case len(c.mapJobs) > 0:
		mapJobID := c.mapJobs[len(c.mapJobs)-1]
		reply.JobType = JobMap
		reply.JobID = mapJobID
		reply.NReduce = c.nReduce
		reply.InpFiles = []string{c.mapInputFiles[mapJobID]}
		reply.OutpFile = fmt.Sprintf("mr-%d-", mapJobID)

		c.mapJobs = c.mapJobs[:len(c.mapJobs)-1]
		log.Printf("[Coordinator]map job %d (%s) is assigned", mapJobID, c.mapInputFiles[mapJobID])
		// spawn a goroutine to monitor the progress of the map job, if it fails
		// (not completed within 10 seconds), re-assign the job by putting it
		// back to `c.mapJobs`.
		go c.monitorMapJob(mapJobID, reply.OutpFile+"*")

	// 2. JobWait
	case len(c.mapJobs) == 0 && len(c.mapCompleted) < c.nMap:
		reply.JobType = JobWait
		log.Println("[Coordinator]no job available, please wait")

	// 3. JobReduce
	case len(c.reduceJobs) > 0:
		reduceJobID := c.reduceJobs[len(c.reduceJobs)-1]
		reply.JobType = JobReduce
		reply.JobID = reduceJobID
		reply.NReduce = c.nReduce
		reduceFiles, _ := filepath.Glob(fmt.Sprintf("mr-*-%d", reduceJobID))
		reply.InpFiles = reduceFiles
		reply.OutpFile = fmt.Sprintf("mr-out-%d", reduceJobID)

		c.reduceJobs = c.reduceJobs[:len(c.reduceJobs)-1]
		log.Printf("[Coordinator]reduce job %d is assigned\n", reduceJobID)
		go c.monitorReduceJob(reduceJobID, reply.OutpFile)

	// 4. JobWait
	case len(c.reduceJobs) == 0 && len(c.reduceCompleted) < c.nReduce:
		reply.JobType = JobWait
		log.Println("[Coordinator]no job available, please wait")

	// 5. JobEnd
	default:
		reply.JobType = JobEnd
		log.Println("[Coordinator]all jobs are completed")
	}
	return nil
}

// monitorMapJob checks if a map job `jobID` is completed within 10 seconds, by
// checking if there're `nReduce` files matching the pattern "mr-jobID-*".
// If completed, it moves the job to `c.mapCompleted`;
// If failed, it puts the job back to `c.mapJobs` for re-assignment.
//
// TODO(djdongjin): a better monitor logic is to do the validation per, e.g., 2
// seconds and fail the job after 10 seconds, instead of sleeping 10 seconds
// directly.
func (c *Coordinator) monitorMapJob(jobID int, filePattern string) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.checkFilePattern(filePattern, c.nReduce) {
		c.mapCompleted[jobID] = struct{}{}
		log.Printf("[Coordinator]map job %d completed\n", jobID)
	} else {
		c.mapJobs = append(c.mapJobs, jobID)
		log.Printf("[Coordinator]map job %d failed, re-assign\n", jobID)
	}
}

// monitorReduceJob checks if a reduce job `jobID` is completed within 10 seconds, by
// checking if there's a file matching the pattern "mr-out-jobID".
// If completed, it moves the job to `c.reduceCompleted` and deletes all intermediate files;
// If failed, it puts the job back to `c.reduceJobs` for re-assignment.
func (c *Coordinator) monitorReduceJob(jobID int, filePattern string) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.checkFilePattern(filePattern, 1) {
		c.reduceCompleted[jobID] = struct{}{}
		cachedFileName, _ := filepath.Glob(fmt.Sprintf("mr-*-%d", jobID))
		for _, fname := range cachedFileName {
			if !strings.Contains(fname, "mr-out") {
				os.Remove(fname)
			}
		}
		log.Printf("[Coordinator]reduce job %d completed\n", jobID)
	} else {
		c.reduceJobs = append(c.reduceJobs, jobID)
		log.Printf("[Coordinator]reduce job %d failed, re-assign\n", jobID)
	}
}

// checkFilePattern is a helper to check if a given file `pattern` matches `count` files.
// If not, it deletes all matched files.
//
// This can be used to validte the completion of a map/reduce job.
// For a map job, `count` should be `nReduce`, and `pattern` should be "mr-JobID-";
// For a reduce job, `count` should be 1, and `pattern` should be "mr-out-JobID".
func (c *Coordinator) checkFilePattern(pattern string, count int) bool {
	matchedNames, err := filepath.Glob(pattern)
	if err != nil {
		return false
	}
	if matchedNames == nil || len(matchedNames) < count {
		for _, nm := range matchedNames {
			if err := os.Remove(nm); err != nil {
				log.Fatalf("[Coordinator]error on deleting file %v\n", nm)
			} else {
				log.Printf("[Coordinator]delete file %v\n", nm)
			}
		}
		return false
	}
	return true
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.mapCompleted) == c.nMap && len(c.reduceCompleted) == c.nReduce {
		log.Println("[Coordinator]all jobs are completed")
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	log.SetOutput(ioutil.Discard)

	// Your code here.
	c.mapInputFiles = files
	c.nMap = len(c.mapInputFiles)
	c.nReduce = nReduce

	// initialize map and reduce job IDs as their indices.
	c.mapJobs = make([]int, len(files))
	for i := range c.mapJobs {
		c.mapJobs[i] = i
	}
	c.reduceJobs = make([]int, nReduce)
	for i := range c.reduceJobs {
		c.reduceJobs[i] = i
	}
	c.mapCompleted = make(map[int]struct{})
	c.reduceCompleted = make(map[int]struct{})

	c.server()
	log.Println("[Coordinator]up and running")
	return &c
}
