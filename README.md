# Labs for MIT's 6.824/6.5840 Distributed System - 2023 Spring

To run a test many times in parallel (for Lab 2 now):

```bash
# go to src/raft
# run the test `500` times, with `32` workers and `2C` as test filter (i.e., value passed to `go test`'s `-run` flag)
$ ./go-test-many.sh 500 32 2C
```

- [x] [Lab 1: MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
- [x] [Lab 2: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html) [pass 99/100 test runs (`./go-test-many.sh 100 20 2`)]
  - [x] Lab 2A: LeaderElection and Heartbeat [pass 1000/1000]
  - [x] Lab 2B: AppendEntries for leaders and followers [pass 1000/1000]
  - [x] Lab 2C: Persist Raft data [pass 497~500/500, rare failure means an optimization might necessary]
  - [x] Lab 2D: log compaction
- [ ] [Lab 3: Key-Value storage based on Raft](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html)
  - [ ] Lab 3A: KV storage without snapshots
  - [ ] Lab 3B: KV storage with snapshots
- [ ] [Lab 4: Sharded Key-Value storage](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)
  - [ ] Lab 4A: The Shard controller
  - [ ] Lab 4B: Sharded Key/Value Server
