package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	OpPut    = "Put"
	OpAppend = "Append"
	OpGet    = "Get"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// To uniquely identify each request from every clerk client,
	// we use a globally-unique `UID` to identify each clerk client,
	// then within each clerk client, we use `RID` (monotonically increase from 0)
	// to identify each request.
	UID int64
	RID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UID int64
	RID int64
}

type GetReply struct {
	Err   Err
	Value string
}
