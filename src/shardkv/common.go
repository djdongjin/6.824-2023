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

	OpPut      = "Put"
	OpAppend   = "Append"
	OpGet      = "Get"
	OpPutShard = "PutShard"
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

type MigrateShardArgs struct {
	ConfigNum int
	Shard     int
}

type MigrateShardReply struct {
	Err Err

	Data    map[string]string
	UID2RID map[int64]int64
	UID2Val map[int64]string
}
