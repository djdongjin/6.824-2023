package raft

const (
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER    = "LEADER"

	TypeAppendEntries = 1
	TypeHeartbeat     = 2
)

const (
	// lab2 requires no more than 10 heartbeats per second
	HEARTBEAT_INTERVAL = 150
	// lab2 requires electing a new leader within 5 seconds.
	// We also need multiple heartbeats before starting a new election (i.e. before timeout).
	ELECTION_TIMEOUT_MIN = 300
	ELECTION_TIMEOUT_JIT = 150
)

// AppendEntriesArgs is a RPC request struct for AppendEntries RPC.
type AppendEntriesArgs struct {
	Type         int
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int

	Entries           []LogEntry // empty for heartbeat
	LeaderCommitIndex int
}

// AppendEntriesReply is a RPC reply from AppendEntries RPC.
type AppendEntriesReply struct {
	Term    int
	Success bool

	// For nextIndex backup optimization
	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
}

// LogEntry is a log entry in the log.
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}
