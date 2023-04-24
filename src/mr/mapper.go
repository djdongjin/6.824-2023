package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
)

func mapper(mapf func(string, string) []KeyValue, reply *MRReply) bool {
	// 1. read input file and generate []KeyValue with `mapf`.
	inpFile := reply.InpFiles[0] // for map tasks, there is only one input file.
	file, err := os.Open(inpFile)
	if err != nil {
		log.Fatalf("[Worker][Mapper]cannot open %v", inpFile)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[Worker][Mapper]cannot read %v", inpFile)
	}
	kva := mapf(inpFile, string(content))

	// 2. sort []KeyValue and output to mr-X-Y files.
	// sort all kv pairs, and hash each key to a specific reduce task.
	sort.Sort(ByKey(kva))
	hash2ikv := make([][]int, reply.NReduce)
	for i := range kva {
		h := ihash(kva[i].Key) % reply.NReduce
		hash2ikv[h] = append(hash2ikv[h], i)
	}

	// write hased/partitioned kv pairs as JSON to mr-X-Y files.
	hash2outpFiles := make([]*os.File, reply.NReduce)
	for i := range hash2outpFiles {
		tmpFile, _ := ioutil.TempFile(".", "*")
		hash2outpFiles[i] = tmpFile
		enc := json.NewEncoder(hash2outpFiles[i])
		for _, ikv := range hash2ikv[i] {
			if err := enc.Encode(&kva[ikv]); err != nil {
				log.Fatal("[Worker][Mapper]encoding error")
			}
		}
		hash2outpFiles[i].Close()
	}

	// 3. rename temp files to mr-X-Y to make the output files visible.
	for i, f := range hash2outpFiles {
		os.Rename(f.Name(), fmt.Sprintf(reply.OutpFile+"%d", i))
	}
	log.Printf("[Worker][Mapper] complete job %d\n", reply.JobID)
	return true
}
