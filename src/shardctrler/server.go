package shardctrler

import (
	"log"
	"sort"
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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead     int32
	killedCh chan struct{}

	maxraftstate int // snapshot if log grows this big

	configs []Config // indexed by config num

	uid2rid    map[int64]int64 // UID -> latest RID from this UID
	uid2config map[int64]int   // UID -> latest value from this UID

	index2uid map[int]int64 // log entry index -> UID

	// for snapshotting
	// `lastIncludedIndex` is the index of the last log entry included in the snapshot.
	// `persister` is only used to calculate raft state size.
	lastIncludedIndex int
	persister         *raft.Persister
}

type Op struct {
	// Your data here.
	Type string
	UID  int64
	RID  int64

	Servers map[int][]string // JoinArgs
	GIDs    []int            // LeaveArgs
	Shard   int              // MoveArgs
	GID     int              // MoveArgs
	Num     int              // QueryArgs
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.Err = sc.sendOpToRaft(Op{Type: OpJoin, UID: args.UID, RID: args.RID, Servers: args.Servers})
	reply.WrongLeader = (reply.Err == ErrWrongLeader)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.Err = sc.sendOpToRaft(Op{Type: OpLeave, UID: args.UID, RID: args.RID, GIDs: args.GIDs})
	reply.WrongLeader = (reply.Err == ErrWrongLeader)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.Err = sc.sendOpToRaft(Op{Type: OpMove, UID: args.UID, RID: args.RID, Shard: args.Shard, GID: args.GID})
	reply.WrongLeader = (reply.Err == ErrWrongLeader)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.Err = sc.sendOpToRaft(Op{Type: OpQuery, UID: args.UID, RID: args.RID, Num: args.Num})
	reply.WrongLeader = (reply.Err == ErrWrongLeader)
	if reply.Err == OK {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		reply.Config = sc.configs[sc.uid2config[args.UID]]
	}
}

func (sc *ShardCtrler) sendOpToRaft(op Op) Err {
	sc.mu.Lock()
	if _, ok := sc.uid2rid[op.UID]; !ok {
		sc.uid2rid[op.UID] = 0
	}
	rid := sc.uid2rid[op.UID]
	switch {
	case op.RID < rid:
		sc.mu.Unlock()
		return ErrWrongLeader
	case op.RID == rid:
		sc.mu.Unlock()
		return OK
	case op.RID > rid:
		// new op, proceed below
		sc.mu.Unlock()
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader
	}
	return sc.waitOpToFinish(index, op.UID, op.RID)
}

func (sc *ShardCtrler) waitOpToFinish(index int, uid, rid int64) Err {
	i := 0
	for !sc.killed() && i < 70 {
		i++

		sc.mu.Lock()
		switch {
		case sc.uid2rid[uid] == rid:
			sc.mu.Unlock()
			return OK
		case sc.uid2rid[uid] > rid:
			sc.mu.Unlock()
			return ErrWrongLeader
		}

		if committedUid, ok := sc.index2uid[index]; ok {
			var err Err
			if committedUid == uid && sc.uid2rid[uid] == rid {
				err = OK
			} else {
				err = ErrWrongLeader
			}
			sc.mu.Unlock()
			return err
		}

		sc.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	return ErrWrongLeader
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case <-sc.killedCh:
			return
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				sc.applyCommand(&msg)
			} else {
				panic("snapshot not implemented")
			}
		}
	}
}

func (sc *ShardCtrler) applyCommand(msg *raft.ApplyMsg) {
	op := msg.Command.(Op)
	index := msg.CommandIndex

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// duplicate `Op`, return directly.
	switch {
	case op.RID < sc.uid2rid[op.UID]:
		DPrintf("[PANIC][applyCommand] %d, found too small RID from %d. expected: %d, actual: %d", sc.me, op.UID, sc.uid2rid[op.UID]+1, op.RID)
		return
	case op.RID == sc.uid2rid[op.UID]:
		DPrintf("[applyCommand] %d, found duplicate RID from %d. expected: %d, actual: %d", sc.me, op.UID, sc.uid2rid[op.UID]+1, op.RID)
		return
	case op.RID == sc.uid2rid[op.UID]+1:
	// expected case
	case op.RID > sc.uid2rid[op.UID]+1:
		DPrintf("[PANIC][applyCommand] %d, found too large RID from %d. expected: %d, actual: %d", sc.me, op.UID, sc.uid2rid[op.UID]+1, op.RID)
		return
	}

	DPrintf("[applyCommand] %d, applying index: %d, UID: %d, RID: %d", sc.me, index, op.UID, op.RID)

	// update sc server data
	sc.index2uid[index] = op.UID

	sc.uid2rid[op.UID] = op.RID
	switch op.Type {
	case OpJoin:
		servers := op.Servers
		curConfig := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    curConfig.Num + 1,
			Shards: curConfig.Shards, // `Shards` is an array instead of a slice, so here we're copying it.
			Groups: copyGroups(curConfig.Groups),
		}
		for gid, s := range servers {
			newConfig.Groups[gid] = s
		}
		sc.configs = append(sc.configs, newConfig)
		sc.shuffleShards()

	case OpLeave:
		gids := op.GIDs
		curConfig := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    curConfig.Num + 1,
			Shards: curConfig.Shards, // `Shards` is an array instead of a slice, so here we're copying it.
			Groups: copyGroups(curConfig.Groups),
		}
		for _, gid := range gids {
			delete(newConfig.Groups, gid)
		}
		sc.configs = append(sc.configs, newConfig)
		sc.shuffleShards()

	case OpMove:
		shard := op.Shard
		gid := op.GID
		curConfig := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    curConfig.Num + 1,
			Shards: curConfig.Shards, // `Shards` is an array instead of a slice, so here we're copying it.
			Groups: copyGroups(curConfig.Groups),
		}
		newConfig.Shards[shard] = gid
		sc.configs = append(sc.configs, newConfig)

	case OpQuery:
		if op.Num == -1 || op.Num >= len(sc.configs) {
			sc.uid2config[op.UID] = len(sc.configs) - 1
		} else {
			sc.uid2config[op.UID] = op.Num
		}
	}

	DPrintf("[applyCommand.End] %d, applying index: %d, UID: %d, RID: %d, command: %s, last config: %+v", sc.me, index, op.UID, op.RID, op.Type, sc.configs[len(sc.configs)-1])
}

// shuffleShards reassigns shards to groups in the latest config
// (i.e. updating `sc.configs[len(sc.configs)-1].Shards`).
// Attention: it should be called with `sc.mu` acquired.
func (sc *ShardCtrler) shuffleShards() {
	idx := len(sc.configs) - 1
	gids := make([]int, 0, len(sc.configs[idx].Groups))
	for gid := range sc.configs[idx].Groups {
		gids = append(gids, gid)
	}

	if len(gids) == 0 {
		for i := 0; i < NShards; i++ {
			sc.configs[idx].Shards[i] = 0
		}
		return
	}

	sort.Ints(gids)
	DPrintf("[shuffleShards] %d, gids: %+v", sc.me, gids)
	shardPerGroup := NShards / len(gids)
	if shardPerGroup == 0 {
		shardPerGroup = 1
	}
	for i := 0; i < NShards; i++ {
		sc.configs[idx].Shards[i] = gids[(i/shardPerGroup)%len(gids)]
	}
}

func copyGroups(g map[int][]string) map[int][]string {
	res := make(map[int][]string)
	for k, v := range g {
		group := make([]string, len(v))
		copy(group, v)
		res[k] = group
	}
	return res
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
	sc.killedCh <- struct{}{}
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.persister = persister
	sc.uid2rid = make(map[int64]int64)
	sc.uid2config = make(map[int64]int)
	sc.index2uid = make(map[int]int64)

	sc.killedCh = make(chan struct{}, 1)

	go sc.applier()

	return sc
}
