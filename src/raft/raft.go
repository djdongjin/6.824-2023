package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh      chan ApplyMsg
	receiveHB    bool // true if receive HB (from leader, or candidate requesting vote and granted)
	voteReceived int  // number of votes received (it may be granted or not)
	voteGranted  int  // number of votes granted

	// Persistent stateson all servers:
	// updated on stable storage before responding to RPCs
	currentTerm int
	votedFor    int
	logs        []LogEntry
	role        string // FOLLOWER, CANDIDATE, LEADER

	// Volatile state on all servers:
	commitIndex int // init to 0, increases monotonically
	lastApplied int // init to 0, increases monotonically

	// Volatile state on leaders:
	// reinitialized after election
	nextIndex  []int // next log entry to send to a follower, init to leader last log index + 1
	matchIndex []int // highest log entry known to be replicated on a follower, init to 0
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == LEADER

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Reply false if term < currentTerm (5.1)
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.logBehindThan(args) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// Reset election timer: grant vote to candidate.
		rf.receiveHB = true
	}

	DPrintf("[RequestVote] %d -> %d (votedFor: %d), request: %+v, reply: %+v", args.CandidateId, rf.me, rf.votedFor, args, reply)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// DPrintf("[sendRequestVote] %d -> %d, request: %+v", args.CandidateId, server, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	index = len(rf.logs)
	isLeader = rf.role == LEADER
	if !isLeader {
		return -1, term, false
	}

	rf.logs = append(rf.logs, LogEntry{
		Term:    term,
		Command: command,
		Index:   index,
	})

	DPrintf("[Start] %d, term: %d, index: %d, command: %+v", rf.me, rf.currentTerm, index, command)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm := rf.logs[prevLogIndex].Term
		entries := rf.logs[prevLogIndex+1:]
		go rf.sendAppendEntriesToOne(i, rf.me, term, rf.commitIndex, prevLogTerm, prevLogIndex, entries, true)
	}

	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker runs in the background to check if a leader election should be started.
// Its main logic is a `for` loop that:
// 1. Reset the `receiveHB` flag;
// 2. Start a randomized election timeout;
// 3. Check if a leader election should be started.
//
// A given `ticker` gorouotine is binded to a follower/candidate in a given term,
// and ends when either the status or the term is changed.
//
// It's created when:
// 1. A Raft instance is created by `Make()` (none -> follower);
// 2. leader -> follower
// 3. candidate -> follower
// 4. follower -> candidate
// 2-4 will end existing `ticker` gorouotine because of the status/term change.
func (rf *Raft) ticker() {
	rf.mu.Lock()
	startTerm, startStatus := rf.currentTerm, rf.role
	rf.mu.Unlock()
	if startStatus == LEADER {
		return
	}

	DPrintf("[ticker.start] %d, term: %d, role: %s\n", rf.me, startTerm, startStatus)

	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.

		// 1. Reset the HB receive flag before starting election timeout.
		rf.mu.Lock()
		rf.receiveHB = false
		rf.mu.Unlock()

		// 2. Randomize the election timeout duration.
		timeout := time.Duration(ELECTION_TIMEOUT_MIN+rand.Int63()%ELECTION_TIMEOUT_JIT) * time.Millisecond
		time.Sleep(timeout)

		// 3. Start leader election if not yet a leader and not receive HB.
		rf.mu.Lock()
		if rf.role == LEADER {
			rf.mu.Unlock()
			DPrintf("[ticker.end] %d, term: %d -> %d, role: %s -> %s\n", rf.me, startTerm, rf.currentTerm, startStatus, rf.role)
			break
		}
		if !rf.receiveHB {
			go rf.startLeaderElection()
		}
		rf.mu.Unlock()
	}
}

// startLeaderElection initiates a leader election by updating the Raft node's
// metadata, and sending RequestVote RPCs to all other nodes.
func (rf *Raft) startLeaderElection() {

	// Lock only the duration of changing the Raft node's metadata, so it can
	// still receive RPCs and potentially discover a leader or a candidate
	// with higher term.
	rf.mu.Lock()
	rf.becomeCandidate(rf.currentTerm + 1)
	// Record necessary metadata for this term's RequestVote RPCs.
	term := rf.currentTerm
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	rf.mu.Unlock()

	DPrintf("[startLeaderElection.start] %d, term: %d\n", rf.me, term)

	var cond = sync.NewCond(&rf.mu)

	// Send RequestVote RPCs to all other nodes.
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		i := i
		request := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogTerm:  lastLogTerm,
			LastLogIndex: lastLogIndex,
		}
		reply := RequestVoteReply{}

		go func() {
			ok := rf.sendRequestVote(i, &request, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.voteReceived++
			if ok {
				if !rf.checkTermAndRole(request.Term, CANDIDATE) {
					return
				}
				if reply.VoteGranted {
					rf.voteGranted++
				}
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
				}
				// don't need `broadcast`, since only the main goroutine is waiting.
				cond.Broadcast()
			}
		}()
	}

	// Check election result whenever a vote is received. Wait while we cannot make a decision:
	// 1. Haven't received a majority of votes.
	// 2. The Raft node's term has been changed (e.g., a leader is found, or a new election is initiated).
	// 3. A higher term is discovered from other nodes' RequestVote RPC replies.
	rf.mu.Lock()
	for rf.checkTermAndRole(term, CANDIDATE) && rf.voteGranted <= len(rf.peers)/2 && rf.voteReceived < len(rf.peers) {
		cond.Wait()
	}
	defer rf.mu.Unlock()

	// If after sending RequestVote RPCs, the Raft node is no longer a candidate,
	// or not in the same term, return directly since this election has expired.

	DPrintf("[startLeaderElection.end] %d, term: %d -> %d, vote granted: %d, vote received: %d\n", rf.me, term, rf.currentTerm, rf.voteGranted, rf.voteReceived)

	switch {
	case !rf.checkTermAndRole(term, CANDIDATE):
		// This election has expired, e.g., observing a new leader or a higher term.
		return
	case rf.voteGranted > len(rf.peers)/2:
		// Win the election
		rf.becomeLeader(rf.currentTerm)
	default:
		// Neither observer newer term nor win the election.
		// Do nothing in this round of leader election.
		// The next election timeout will start a new leader election.
	}
}

func (rf *Raft) tickerHB() {
	rf.mu.Lock()
	startTerm := rf.currentTerm
	rf.mu.Unlock()

	DPrintf("[tickerHB.start] %d, term: %d\n", rf.me, startTerm)

	for rf.killed() == false {
		// Periodically send heartbeat to all other nodes.

		// 1. Sleep for a heartbeat interval.
		timeout := time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond
		time.Sleep(timeout)
		rf.mu.Lock()
		if !rf.checkTermAndRole(startTerm, LEADER) {
			DPrintf("[tickerHB.end] %d, term: %d -> %d, role: %s -> %s\n", rf.me, startTerm, rf.currentTerm, LEADER, rf.role)
			rf.mu.Unlock()
			break
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			prevLogIndex := rf.nextIndex[i] - 1
			prevLogTerm := rf.logs[prevLogIndex].Term
			entries := rf.logs[prevLogIndex+1:]
			go rf.sendAppendEntriesToOne(i, rf.me, rf.currentTerm, rf.commitIndex, prevLogTerm, prevLogIndex, entries, false)
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntriesToOne(i, leaderId, term, leaderCommitIndex, prevLogTerm, prevLogIndex int, entries []LogEntry, retry bool) {
	request := AppendEntriesArgs{
		Term:              term,
		LeaderId:          leaderId,
		LeaderCommitIndex: leaderCommitIndex,
		PrevLogTerm:       prevLogTerm,
		PrevLogIndex:      prevLogIndex,
		Entries:           entries,
	}

	for !rf.killed() {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(i, &request, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		if !rf.checkTermAndRole(request.Term, LEADER) {
			// Term + role has been changed, return without processing reply.
			rf.mu.Unlock()
			return
		}

		if reply.Term > rf.currentTerm {
			// Find new term, convert to follower and return.
			rf.becomeFollower(reply.Term)
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			// Update nextIndex and matchIndex for follower.
			matchIndex := request.PrevLogIndex + len(request.Entries)
			if rf.matchIndex[i] < matchIndex {
				// trick: if `rf.matchIndex[i]` is already larger, that
				// means it has been updated by newer AE RPC replies.
				// The current reply, although success (i.e. contains a subset of logs),
				// contains outdated information about matchIndex and nextIndex. In
				// that case, we don't need to update it, which makes it smaller and
				// makes latter AE RPCs contains more entries.
				rf.matchIndex[i] = matchIndex
				rf.nextIndex[i] = matchIndex + 1
			}

			if rf.commitIndex < matchIndex && rf.currentTerm == rf.logs[matchIndex].Term {
				cnt := 1 // leader itself
				for j := range rf.peers {
					if j != rf.me && rf.matchIndex[j] >= matchIndex {
						cnt++
					}
					if cnt > len(rf.peers)/2 {
						rf.commitIndex = matchIndex
						go rf.applyCommit()
						break
					}
				}
			}

			rf.mu.Unlock()
			return
		}

		// Retry with smaller `nextIndex`.
		rf.nextIndex[i]--
		request.PrevLogIndex = rf.nextIndex[i] - 1
		request.PrevLogTerm = rf.logs[request.PrevLogIndex].Term
		request.Entries = rf.logs[rf.nextIndex[i]:]
		rf.mu.Unlock()
		if !retry {
			return
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("[sendAppendEntries] %d -> %d, request: %+v", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// 1. Reply false directly if term < currentTerm.
	if args.Term < rf.currentTerm {
		return
	}

	// Reset election timer: receive AE RPC from current leader that the node knows.
	rf.receiveHB = true

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm.
	if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	// 3. & 4.
	reply.Success = true
	for i, ent := range args.Entries {
		// 3. If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it.
		if ent.Index < len(rf.logs) && rf.logs[ent.Index].Term != ent.Term {
			rf.logs = rf.logs[:ent.Index]
		}
		// 4. Append any new entries NOT ALREADY in the log.
		if ent.Index >= len(rf.logs) {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		}
		// 3. ensures `rf.logs` doesn't have conflict entries, and 4. ensures `rf.logs` has all new entries.
		// So if we're here, we can go to the next loop (i.e., the current entry already exists in `rf.logs`).
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
	DPrintf("leader commit index: %d, current commit index: %d, log len:%d", args.LeaderCommitIndex, rf.commitIndex, len(rf.logs))
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = rf.logs[len(rf.logs)-1].Index
		if rf.commitIndex > args.LeaderCommitIndex {
			rf.commitIndex = args.LeaderCommitIndex
		}
		go rf.applyCommit()
	}

	DPrintf("[AppendEntries] %d -> %d, request: %+v, reply: %+v, log: %v", args.LeaderId, rf.me, args, reply, rf.logs)
}

func (rf *Raft) lastLogTermIndex() (int, int) {
	lastLog := rf.logs[len(rf.logs)-1]
	return lastLog.Term, lastLog.Index
}

// logBehindThan checks if the voter's log is behind or equal to the candidate's log.
func (rf *Raft) logBehindThan(args *RequestVoteArgs) bool {
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	return args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
}

func (rf *Raft) becomeCandidate(term int) {
	DPrintf("[becomeCandidate] %d, term: %d -> %d, role: %s -> %s\n", rf.me, rf.currentTerm, term, rf.role, CANDIDATE)
	rf.role = CANDIDATE
	rf.currentTerm = term
	rf.votedFor = rf.me
	rf.voteReceived = 1
	rf.voteGranted = 1
}

func (rf *Raft) becomeFollower(term int) {
	DPrintf("[becomeFollower] %d, term: %d -> %d, role: %s -> %s\n", rf.me, rf.currentTerm, term, rf.role, FOLLOWER)
	// only changing from `LEADER` to `FOLLOWER` needs to restart the ticker.
	if rf.role == LEADER {
		go rf.ticker()
	}
	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) becomeLeader(term int) {
	DPrintf("[becomeLeader] %d, term: %d -> %d, role: %s -> %s\n", rf.me, rf.currentTerm, term, rf.role, LEADER)
	rf.role = LEADER
	rf.currentTerm = term
	// Doesn't set/reset `votedFor` because to become a leader of `term`, it must
	// have voted for itself.
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
	go rf.tickerHB()
}

func (rf *Raft) checkTermAndRole(term int, role string) bool {
	return rf.currentTerm == term && rf.role == role
}

func (rf *Raft) applyCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[applyCommit] %d, term: %d, apply log [%d, %d]", rf.me, rf.currentTerm, rf.lastApplied+1, rf.commitIndex)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: rf.logs[rf.lastApplied].Index,
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.receiveHB = false

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []LogEntry{{Command: nil, Term: 0, Index: 0}}
	rf.role = FOLLOWER

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
