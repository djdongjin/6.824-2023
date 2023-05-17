package raft

const (
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER    = "LEADER"
)

const (
	// lab2 requires no more than 10 heartbeats per second
	HEARTBEAT_INTERVAL = 150
	// lab2 requires electing a new leader within 5 seconds.
	// We also need multiple heartbeats before starting a new election (i.e. before timeout).
	ELECTION_TIMEOUT_MIN = 300
	ELECTION_TIMEOUT_JIT = 150
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	// candidate's last log entry.
	// they are used to ensure election restriction (paper 5.4.1).
	// a candidate receives vote for a follower only if the candidate's log
	// is more up-to-date than the follower's.
	// "up-to-date" is measured by (term, index) of the last log entry.
	LastLogTerm  int
	LastLogIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

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

// InstallSnapshotArgs is a RPC request struct for InstallSnapshot RPC.
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply is a RPC reply from InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term int // currentTerm of the receiving server, for leader to update itself if necessary
}
