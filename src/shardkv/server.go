package shardkv

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const Debug = false

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
	dead int32 // set by Kill()
	mck  *shardctrler.Clerk

	shards    map[int]*Shard
	inShards  map[int]struct{}
	outShards map[int]*Shard

	index2uid map[int]int64

	killedCh          chan struct{}
	lastIncludedIndex int
	persister         *raft.Persister

	config     shardctrler.Config
	prevConfig shardctrler.Config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Value, reply.Err = kv.sendOpToRaft(OpFromGetArgs(args), args.ConfigNum)
	if reply.Err == OK && reply.Value == "" {
		reply.Err = ErrNoKey
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, reply.Err = kv.sendOpToRaft(OpFromPutAppendArgs(args), args.ConfigNum)
}

func (kv *ShardKV) sendOpToRaft(op Op, configNum int) (string, Err) {
	shardNum := key2shard(op.Key)
	DPrintf("[sendOpToRaft] %d, shard: %d, client config num: %d, op: %+v", kv.me, shardNum, configNum, op)

	// first ensure we have config newer than `configNum` AND the shard is
	// not waiting to be pulled from other groups.
	waitConfigAndShard := true
	for waitConfigAndShard {
		if kv.killed() {
			return "", ErrWrongLeader
		}

		kv.mu.Lock()
		if _, ok := kv.inShards[shardNum]; ok || kv.config.Num < configNum {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		} else {
			waitConfigAndShard = false
		}
	}

	// then, with lock held, we check if we are accessing a shard in other groups.
	if kv.config.Shards[shardNum] != kv.gid {
		kv.mu.Unlock()
		return "", ErrWrongGroup
	}

	shard, ok := kv.shards[shardNum]
	if !ok {
		shard = NewShard(shardNum, kv.config.Num)
		kv.shards[shardNum] = shard
	}

	rid := shard.UID2RID[op.UID]
	switch {
	case op.RID < rid:
		kv.mu.Unlock()
		DPrintf("[PANIC][sendOpToRaft] %d, too small RID from UID %d. expected: >= %d, actual: %d", kv.me, op.UID, rid, op.RID)
		return "", ErrWrongLeader
	case op.RID == rid:
		val := shard.UID2Val[op.UID]
		kv.mu.Unlock()
		return val, OK
	case op.RID > rid:
		// new op, proceed below
		kv.mu.Unlock()
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", ErrWrongLeader
	}

	i := 0
	uid, rid := op.UID, op.RID
	for !kv.killed() && i < 70 {
		i++

		kv.mu.Lock()
		switch {
		case kv.config.Shards[shardNum] != kv.gid:
			kv.mu.Unlock()
			return "", ErrWrongGroup
		case shard.UID2RID[uid] == rid:
			val := shard.UID2Val[uid]
			kv.mu.Unlock()
			return val, OK
		case shard.UID2RID[uid] > rid:
			kv.mu.Unlock()
			return "", ErrWrongLeader
		}

		if committedUid, ok := kv.index2uid[index]; ok {
			var err Err
			var val string
			if committedUid == uid && shard.UID2RID[uid] == rid {
				val = shard.UID2Val[uid]
				err = OK
			} else {
				err = ErrWrongLeader
			}
			kv.mu.Unlock()
			return val, err
		}

		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	return "", ErrWrongLeader
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
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
	labgob.Register(shardctrler.Config{})
	labgob.Register(Shard{})
	labgob.Register(CleanShard{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.persister = persister
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.shards = make(map[int]*Shard)
	kv.inShards = make(map[int]struct{})
	kv.outShards = make(map[int]*Shard)

	kv.index2uid = make(map[int]int64)

	kv.killedCh = make(chan struct{}, 1)

	kv.readSnapshot(kv.persister.ReadSnapshot())
	go kv.applier()
	go kv.configPuller()
	go kv.shardFetcher()

	return kv
}
