package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sm     *shardctrler.Clerk
	config shardctrler.Config

	shard2data map[int]map[string]string // shard -> key-value store
	// duplicate detection table:
	shard2uid2rid map[int]map[int64]int64  // shard -> (UID -> latest RID from this UID)
	shard2uid2val map[int]map[int64]string // shard -> (UID -> latest value from this UID)
	// `index2uid` makes it possible to detect request failure faster.
	index2uid map[int]int64 // log entry index -> UID

	dead     int32 // set by Kill()
	killedCh chan struct{}

	// for snapshotting
	// `lastIncludedIndex` is the index of the last log entry included in the snapshot.
	// `persister` is only used to calculate raft state size.
	lastIncludedIndex int
	persister         *raft.Persister
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Value, reply.Err = kv.sendOpToRaft(OpFromGetArgs(args), args.ConfigNum)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, reply.Err = kv.sendOpToRaft(OpFromPutAppendArgs(args), args.ConfigNum)
}

// waitConfigUpdate ensures the server's config is at least newer than `configNum`.
func (kv *ShardKV) waitConfigUpdate(configNum int) {
	for !kv.killed() {
		kv.mu.Lock()
		if kv.config.Num >= configNum {
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) isWrongGroup(shard int) bool {
	// now we have configNum <= kv.config.configNum
	return kv.config.Shards[shard] != kv.gid
}

func (kv *ShardKV) sendOpToRaft(op Op, configNum int) (string, Err) {
	kv.waitConfigUpdate(configNum)
	shard := key2shard(op.Key)

	for !kv.killed() {
		kv.mu.Lock()
		if kv.isWrongGroup(shard) {
			kv.mu.Unlock()
			return "", ErrWrongGroup
		}

		if _, ok := kv.shard2data[shard]; !ok {
			// data not arrived yet
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if _, ok := kv.shard2uid2rid[shard][op.UID]; !ok {
			kv.shard2uid2rid[shard][op.UID] = 0
		}

		rid := kv.shard2uid2rid[shard][op.UID]
		switch {
		case op.RID < rid:
			kv.mu.Unlock()
			return "", ErrWrongLeader
		case op.RID == rid:
			kv.mu.Unlock()
			return kv.shard2uid2val[shard][op.UID], OK
		case op.RID > rid:
			// new op, proceed below
			kv.mu.Unlock()
		}
		break
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", ErrWrongLeader
	}

	i := 0
	for !kv.killed() && i < 70 {
		i++

		kv.mu.Lock()
		if _, ok := kv.shard2data[shard]; !ok {
			return "", ErrWrongGroup
		}

		switch {
		case kv.shard2uid2rid[shard][op.UID] == op.RID:
			kv.mu.Unlock()
			return kv.shard2uid2val[shard][op.UID], OK
		case kv.shard2uid2rid[shard][op.UID] > op.RID:
			kv.mu.Unlock()
			return "", ErrWrongLeader
		}

		if committedUid, ok := kv.index2uid[index]; ok {
			if committedUid == op.UID && kv.shard2uid2rid[shard][op.UID] == op.RID {
				kv.mu.Unlock()
				return kv.shard2uid2val[shard][op.UID], OK
			} else {
				kv.mu.Unlock()
				return "", ErrWrongLeader
			}
		}

		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}

	return "", ErrWrongLeader
}

func (kv *ShardKV) applier() {
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

func (kv *ShardKV) applyCommand(msg *raft.ApplyMsg) {
	op := msg.Command.(Op)
	index := msg.CommandIndex
	shard := key2shard(op.Key)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.shard2data[shard]; !ok {
		// shard has been moved to another replica group
		return
	}

	// duplicate `Op`, return directly.
	curUid := kv.shard2uid2rid[shard][op.UID]
	switch {
	case op.RID < curUid:
		DPrintf("[PANIC][applyCommand] %d, found too small RID from %d. expected: %d, actual: %d", kv.me, op.UID, curUid+1, op.RID)
		return
	case op.RID == curUid:
		DPrintf("[applyCommand] %d, found duplicate RID from %d. expected: %d, actual: %d", kv.me, op.UID, curUid+1, op.RID)
		return
	case op.RID == curUid+1:
	// expected case
	case op.RID > curUid+1:
		DPrintf("[PANIC][applyCommand] %d, found too large RID from %d. expected: %d, actual: %d", kv.me, op.UID, curUid+1, op.RID)
		return
	}

	DPrintf("[applyCommand] %d, applying index: %d, UID: %d, RID: %d", kv.me, index, op.UID, op.RID)

	// update kv server data
	kv.index2uid[index] = op.UID

	kv.shard2uid2rid[shard][op.UID] = op.RID
	switch op.Type {
	case OpGet:
		if _, ok := kv.shard2data[shard][op.Key]; !ok {
			kv.shard2uid2val[shard][op.UID] = ""
		} else {
			kv.shard2uid2val[shard][op.UID] = kv.shard2data[shard][op.Key]
		}
	case OpPut:
		kv.shard2data[shard][op.Key] = op.Value
	case OpAppend:
		if _, ok := kv.shard2data[shard][op.Key]; !ok {
			kv.shard2data[shard][op.Key] = ""
		}
		kv.shard2data[shard][op.Key] += op.Value
	}

	if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		kv.lastIncludedIndex = index
		kv.rf.Snapshot(index, kv.makeSnapshot())
	}
}

func (kv *ShardKV) applySnapshot(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[applySnapshot.Start] %d, applying snapshot: %d", kv.me, msg.SnapshotIndex)

	if kv.lastIncludedIndex >= msg.SnapshotIndex {
		return
	}

	kv.readSnapshot(msg.Snapshot)

	DPrintf("[applySnapshot.End] %d, applying snapshot: %d", kv.me, msg.SnapshotIndex)
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[int]map[string]string
	var uid2rid map[int]map[int64]int64
	var uid2val map[int]map[int64]string
	var lastIncludedIndex int
	if d.Decode(&data) != nil || d.Decode(&uid2rid) != nil || d.Decode(&uid2val) != nil || d.Decode(&lastIncludedIndex) != nil {
		panic("failed to decode snapshot")
	} else {
		kv.shard2data = data
		kv.shard2uid2rid = uid2rid
		kv.shard2uid2val = uid2val
		kv.lastIncludedIndex = lastIncludedIndex
	}
}

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shard2data)
	e.Encode(kv.shard2uid2rid)
	e.Encode(kv.shard2uid2val)
	e.Encode(kv.lastIncludedIndex)
	data := w.Bytes()
	DPrintf("[makeSnapshot] %d, making snapshot: %d, snapshot size: %d", kv.me, kv.lastIncludedIndex, len(data))
	return data
}

func (kv *ShardKV) configDetector() {
	for !kv.killed() {
		config := kv.sm.Query(-1)
		kv.mu.Lock()
		if config.Num > kv.config.Num {
			kv.reconfigure(config)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) reconfigure(newConfig shardctrler.Config) {
	oldConfig := kv.config
	if newConfig.Num <= oldConfig.Num {
		DPrintf("[PANIC][reconfigure] %d, unexpected config, expected: > %d, actual: %d", kv.me, oldConfig.Num, newConfig.Num)
		return
	}

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	kv.killedCh <- struct{}{}
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)

	kv.shard2data = make(map[int]map[string]string)
	kv.shard2uid2rid = make(map[int]map[int64]int64)
	kv.shard2uid2val = make(map[int]map[int64]string)
	kv.index2uid = make(map[int]int64)

	kv.killedCh = make(chan struct{}, 1)

	kv.readSnapshot(kv.persister.ReadSnapshot())

	go kv.applier()
	go kv.configDetector()

	return kv
}
