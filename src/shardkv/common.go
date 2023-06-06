package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"

	OpPut    = "Put"
	OpAppend = "Append"
	OpGet    = "Get"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UID       int64
	RID       int64
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UID       int64
	RID       int64
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}

type FetchShardArgs struct {
	ShardNum  int
	ConfigNum int
}

type FetchShardReply struct {
	Sueecss bool
	Shard   Shard
}

type CleanShardArgs struct {
	ShardNum  int
	ConfigNum int
}

type CleanShardReply struct {
	Success bool
}

type Shard struct {
	Num       int
	ConfigNum int
	Data      map[string]string
	UID2RID   map[int64]int64
	UID2Val   map[int64]string
}

func NewShard(num, configNum int) *Shard {
	return &Shard{
		Num:       num,
		ConfigNum: configNum,
		Data:      make(map[string]string),
		UID2RID:   make(map[int64]int64),
		UID2Val:   make(map[int64]string),
	}
}

func (s *Shard) Copy() *Shard {
	shard := NewShard(s.Num, s.ConfigNum)
	for k, v := range s.Data {
		shard.Data[k] = v
	}
	for k, v := range s.UID2RID {
		shard.UID2RID[k] = v
	}
	for k, v := range s.UID2Val {
		shard.UID2Val[k] = v
	}
	return shard
}

type CleanShard struct {
	Num       int
	ConfigNum int
}
