package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Op is the `Command` struct in Raft that `KVServer` sends to Raft for consensus,
// and Raft sends back for commit/apply after consensus is reached.
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string // "Put" or "Append" or "Get"
	Key   string
	Value string
	UID   int64
	RID   int64
}

func OpFromGetArgs(args *GetArgs) Op {
	return Op{Type: OpGet, Key: args.Key, UID: args.UID, RID: args.RID}
}

func OpFromPutAppendArgs(args *PutAppendArgs) Op {
	return Op{Type: args.Op, Key: args.Key, Value: args.Value, UID: args.UID, RID: args.RID}
}

// KVServer receives requests from Clerk and sends `Op` to Raft for consensus.
// After consensus is reached (KVServer receives `ApplyMsg` from Raft), KVServer
// sends the result back to Clerk.
// KVServer will fail a Clerk request if:
// 1. Raft cannot reach consensus after a fixed number of retries.
// 2. KVServer notices that Raft committed a log entry with the same index, but
// different `UID`+`RID`, meaning the other ones will be failed utimately.
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// `data` stores the latest key-value pairs with all committed/applied log entries.
	// `op2status` stores the status of each request (`RID`) from every clerk client (`UID`).
	// `op2value` stores the value of each request (`RID`) from every clerk client (`UID`),
	// which is used to detect duplicate requests.
	data map[string]string // key-value store

	// duplicate detection table:
	// `uid2rid` stores the latest `RID` from every clerk client (`UID`), so no duplicate
	// requests from the same client will be applied when receiving `ApplyMsg` from Raft.
	// `uid2val` stores the latest value, so when a duplication did happen, the server
	// can return the value directly.
	uid2rid map[int64]int64  // UID -> latest RID from this UID
	uid2val map[int64]string // UID -> latest value from this UID

	// `index2uid` makes it possible to detect request failure faster.
	// If an `ApplyMsg` is received and its `UID` conflicts with the one in `index2uid`,
	// that means the request from `index2uid[index]` has failed, since a conflicting
	// command has been committed in the same index.
	index2uid map[int]int64 // log entry index -> UID

	killedCh chan struct{}

	// for snapshotting
	// `lastIncludedIndex` is the index of the last log entry included in the snapshot.
	// `persister` is only used to calculate raft state size.
	lastIncludedIndex int
	persister         *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = kv.sendOpToRaft(OpFromGetArgs(args))
	if reply.Err == OK {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Value = kv.uid2val[args.UID]
		if reply.Value == "" {
			reply.Err = ErrNoKey
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = kv.sendOpToRaft(OpFromPutAppendArgs(args))
}

func (kv *KVServer) sendOpToRaft(op Op) Err {
	kv.mu.Lock()
	if _, ok := kv.uid2rid[op.UID]; !ok {
		kv.uid2rid[op.UID] = 0
	}
	rid := kv.uid2rid[op.UID]
	switch {
	case op.RID < rid:
		kv.mu.Unlock()
		DPrintf("[PANIC][sendOpToRaft] %d, too small RID from UID %d. expected: >= %d, actual: %d", kv.me, op.UID, rid, op.RID)
		return ErrWrongLeader
	case op.RID == rid:
		kv.mu.Unlock()
		return OK
	case op.RID > rid:
		// new op, proceed below
		kv.mu.Unlock()
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader
	}
	return kv.waitOpToFinish(index, op.UID, op.RID)
}

func (kv *KVServer) waitOpToFinish(index int, uid, rid int64) Err {
	i := 0
	for !kv.killed() && i < 70 {
		i++

		kv.mu.Lock()
		switch {
		case kv.uid2rid[uid] == rid:
			kv.mu.Unlock()
			return OK
		case kv.uid2rid[uid] > rid:
			kv.mu.Unlock()
			return ErrWrongLeader
		}

		if committedUid, ok := kv.index2uid[index]; ok {
			var err Err
			if committedUid == uid && kv.uid2rid[uid] == rid {
				err = OK
			} else {
				err = ErrWrongLeader
			}
			kv.mu.Unlock()
			return err
		}

		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	return ErrWrongLeader
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killedCh <- struct{}{}
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case <-kv.killedCh:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.applyCommand(&msg)
			} else if msg.SnapshotValid {
				kv.applySnapshot(&msg)
			} else {
				panic("unknown message type")
			}
		}
	}
}

// applyCommand applies an `ApplyMsg` received from Raft. While applying, the following may happen:
// 1. A brand new message that the server never sees (e.g. as a follower)
// 2. The message is the response for an `Op` initiated by the server (e.g. as a leader)
// 3. The message conflict with an ongoing `Op` initiated by the server (e.g. as an expired leader)
func (kv *KVServer) applyCommand(msg *raft.ApplyMsg) {
	op := msg.Command.(Op)
	index := msg.CommandIndex

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// duplicate `Op`, return directly.
	switch {
	case op.RID < kv.uid2rid[op.UID]:
		DPrintf("[PANIC][applyCommand] %d, found too small RID from %d. expected: %d, actual: %d", kv.me, op.UID, kv.uid2rid[op.UID]+1, op.RID)
		return
	case op.RID == kv.uid2rid[op.UID]:
		DPrintf("[applyCommand] %d, found duplicate RID from %d. expected: %d, actual: %d", kv.me, op.UID, kv.uid2rid[op.UID]+1, op.RID)
		return
	case op.RID == kv.uid2rid[op.UID]+1:
	// expected case
	case op.RID > kv.uid2rid[op.UID]+1:
		DPrintf("[PANIC][applyCommand] %d, found too large RID from %d. expected: %d, actual: %d", kv.me, op.UID, kv.uid2rid[op.UID]+1, op.RID)
		return
	}

	DPrintf("[applyCommand] %d, applying index: %d, UID: %d, RID: %d", kv.me, index, op.UID, op.RID)

	// update kv server data
	kv.index2uid[index] = op.UID

	kv.uid2rid[op.UID] = op.RID
	switch op.Type {
	case OpGet:
		if _, ok := kv.data[op.Key]; !ok {
			kv.uid2val[op.UID] = ""
		} else {
			kv.uid2val[op.UID] = kv.data[op.Key]
		}
	case OpPut:
		kv.data[op.Key] = op.Value
	case OpAppend:
		if _, ok := kv.data[op.Key]; !ok {
			kv.data[op.Key] = ""
		}
		kv.data[op.Key] += op.Value
	}

	if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		kv.lastIncludedIndex = index
		kv.rf.Snapshot(index, kv.makeSnapshot())
	}
}

func (kv *KVServer) applySnapshot(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[applySnapshot.Start] %d, applying snapshot: %d", kv.me, msg.SnapshotIndex)

	if kv.lastIncludedIndex >= msg.SnapshotIndex {
		return
	}

	kv.readSnapshot(msg.Snapshot)

	DPrintf("[applySnapshot.End] %d, applying snapshot: %d", kv.me, msg.SnapshotIndex)
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[string]string
	var uid2rid map[int64]int64
	var uid2val map[int64]string
	var lastIncludedIndex int
	if d.Decode(&data) != nil || d.Decode(&uid2rid) != nil || d.Decode(&uid2val) != nil || d.Decode(&lastIncludedIndex) != nil {
		panic("failed to decode snapshot")
	} else {
		kv.data = data
		kv.uid2rid = uid2rid
		kv.uid2val = uid2val
		kv.lastIncludedIndex = lastIncludedIndex
	}
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.uid2rid)
	e.Encode(kv.uid2val)
	e.Encode(kv.lastIncludedIndex)
	data := w.Bytes()
	DPrintf("[makeSnapshot] %d, making snapshot: %d, snapshot size: %d", kv.me, kv.lastIncludedIndex, len(data))
	return data
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.uid2rid = make(map[int64]int64)
	kv.uid2val = make(map[int64]string)
	kv.index2uid = make(map[int]int64)

	kv.killedCh = make(chan struct{}, 1)

	kv.readSnapshot(kv.persister.ReadSnapshot())

	go kv.applier()

	return kv
}
