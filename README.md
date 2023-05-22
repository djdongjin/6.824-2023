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
- [x] [Lab 3: Key-Value storage based on Raft](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html) [pass 100/100 test runs (`./go-test-many.sh 100 20 3`)]
  - [x] Lab 3A: KV storage without snapshots [pass 100/100 test runs (`./go-test-many.sh 100 20 3A`)]
  - [x] Lab 3B: KV storage with snapshots [pass 100/100 test runs (`./go-test-many.sh 100 20 3B`)]
- [ ] [Lab 4: Sharded Key-Value storage](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)
  - [x] Lab 4A: The Shard controller [pass 100/100 test runs (`./go-test-many.sh 100 20`)]
  - [ ] Lab 4B: Sharded Key/Value Server

## Lab 3A Notes

- To make it easier to achieve linearizability, ensure only one outstanding RPC per client/clerk.
You can achieve this by locking whole `Get/PutAppend` calls.
- Keeping only one outstanding RPC per client/clerk also makes the duplication table smaller.
You just need to keep the latest committed request id per client/clerk, instead of all committed request ids.
- Duplicated client requests may happen at two places: (1). previous request is not committed (e.g., failed at server or Raft module); (2). previous request is committed, but the reply is lost (e.g., failed at network).
- When applying a message from Raft, you need to check if it's a duplicate, by checking if the request id is smaller than or equal to the latest committed request id.
- Before sending a request from server to its Raft module, you need to check if it's a duplicate, by checking if the request id is smaller than or equal to the latest committed request id.

## Lab 3B Notes

- If you need to send an `InstallSnapshot` RPC before sending an `AppendEntries` RPC (e.g. `rf.nextIndex[i] <= rf.lastIncludedIndex`), you need to re-check the condition after the `InstallSnapshot` RPC returns.
Otherwise, there might be a new snapshot made right after your `InstallSnapshot` RPC which increase `rf.lastIncludedIndex` again.
