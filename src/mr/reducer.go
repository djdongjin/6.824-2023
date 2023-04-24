package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
)

func reducer(reducef func(string, []string) string, reply *MRReply) bool {
	// 1. read all intermediate files, and generate the `[]KeyValue` to be used by `reducef`.
	intermediate := []KeyValue{}
	for _, filename := range reply.InpFiles {
		ifile, _ := os.Open(filename)
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ifile.Close()
	}

	// 2. sort all intermediate key-value pairs by key, and call `reducef` on each key and its values.
	ofile, _ := ioutil.TempFile(".", "*")
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		// calculate all values associated with the same key.
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	// 3. rename the temporary output file to the final output file from the reduce job.
	os.Rename(ofile.Name(), reply.OutpFile)
	log.Printf("[Worker][Reducer]complete job %d\n", reply.JobID)
	return true
}
