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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

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

	// Persistent state on all servers:
	// updated on stable storage before responding to RPCs
	currentTerm int
	votedFor    int
	logs        []LogEntry
	role        string // FOLLOWER, CANDIDATE, LEADER
	// Persistent state - (2D) Snapshot
	// They're all persistent states because they're the same as `logs` but
	// just in a different format (snapshot). Upon recovery, the state machine
	// need `snapshot` to restore its state.
	//
	// `snapshot` is the state machine's state in a snapshot format.
	snapshot []byte
	// `lastIncludedTerm` and `snapshotIndex` are the term and index of the last log
	// entry in the snapshot. They will be the `PrevLogTerm` and `PrevLogIndex`
	// of the first log entry after the snapshot, when leader sends `AppendEntries`
	// RPC. Also when `nextIndex[i]` decreases to `snapshotIndex`, leader should
	// send a `InstallSnapshot` RPC to follower `i`.
	lastIncludedTerm  int
	lastIncludedIndex int

	// Volatile state on all servers:
	commitIndex int // init to 0, increases monotonically
	lastApplied int // init to 0, increases monotonically
	// We have a single applier goroutine per Raft server that applies logs/snapshots
	// to the state machine. Other goroutines communicate with applier via the 2 channels.
	//
	// Whenever `commitIndex` is changed, we should send a message to `applyCommitCh` to
	// notify applier goroutine to start applying logs.
	//
	// `rf.Kill` sends a message to `killedCh` to notify applier goroutine to exit.
	applyCommitCh chan struct{}
	killedCh      chan struct{}

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
//
// Attention: this function assumes `rf.mu` is already acquired by the caller.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry
	var lastIncludedTerm, lastIncludedIndex int
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if err := d.Decode(&currentTerm); err != nil {
		panic("readPersist error:" + err.Error())
	}
	if err := d.Decode(&votedFor); err != nil {
		panic("readPersist error:" + err.Error())
	}
	if err := d.Decode(&logs); err != nil {
		panic("readPersist error:" + err.Error())
	}
	if err := d.Decode(&lastIncludedTerm); err != nil {
		panic("readPersist error:" + err.Error())
	}
	if err := d.Decode(&lastIncludedIndex); err != nil {
		panic("readPersist error:" + err.Error())
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
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

	isLeader = rf.role == LEADER
	if !isLeader {
		return -1, rf.currentTerm, false
	}

	_, lastLogIndex := rf.lastLogTermIndex()
	term, index = rf.currentTerm, lastLogIndex+1
	rf.logs = append(rf.logs, LogEntry{
		Term:    term,
		Command: command,
		Index:   index,
	})
	rf.persist()

	DPrintf("[Start] %d, term: %d, index: %d, command: %+v", rf.me, rf.currentTerm, index, command)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.sendAppendEntriesToOne(i, true)
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
	rf.killedCh <- struct{}{}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
			rf.mu.Unlock()
			return
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go rf.sendAppendEntriesToOne(i, false)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntriesToOne(i int, retry bool) {
	rf.mu.Lock()
	request := AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LeaderCommitIndex: rf.commitIndex,
		// Remaining fields will be added/updated in the for loop.
	}
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			rf.mu.Unlock()
			// Send snapshot in the same goroutine, since there is nothing to
			// do for it before the snapshot is processed by the receiver.
			rf.sendInstallSnapshotToOne(i)
			rf.mu.Lock()
		}
		if !rf.checkTermAndRole(request.Term, LEADER) {
			rf.mu.Unlock()
			return
		}

		request.PrevLogTerm, request.PrevLogIndex = rf.prevLogTermIndex(rf.nextIndex[i])
		request.Entries = rf.logs[rf.logIdx(rf.nextIndex[i]):]
		reply := AppendEntriesReply{}
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(i, &request, &reply)
		if !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		rf.mu.Lock()
		if !rf.checkTermAndRole(request.Term, LEADER) {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
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

			if rf.commitIndex < matchIndex && rf.currentTerm == rf.logs[rf.logIdx(matchIndex)].Term {
				cnt := 1 // leader itself
				for j := range rf.peers {
					if j != rf.me && rf.matchIndex[j] >= matchIndex {
						cnt++
					}
					if cnt > len(rf.peers)/2 {
						rf.commitIndex = matchIndex
						go rf.startApplyCommit()
						break
					}
				}
			}

			rf.mu.Unlock()
			return
		}

		// Retry with smaller `nextIndex`.
		// Only decrement `nextIndex` if it's not changed during this RPC call,
		// because it might already be updated by other AE RPCs.
		if rf.nextIndex[i] != request.PrevLogIndex+1 {
			rf.mu.Unlock()
			return
		}
		if reply.XIndex != 0 {
			// If the follower has a conflict entry (Xterm, which starts at XIndex) with the leader, it means the whole
			// term will be conflict with the leader at least to `XIndex`. So we first decrease `nextIndex` to `XIndex`,
			// then if in leader's log, if it still has >= XTerm entries, we can continue to decrement `nextIndex`.
			nextIndex := reply.XIndex
			for (nextIndex-1 > rf.lastIncludedIndex && rf.logs[rf.logIdx(nextIndex-1)].Term >= reply.Term) ||
				(nextIndex-1 == rf.lastIncludedIndex && rf.lastIncludedTerm >= reply.Term) {
				nextIndex--
			}
			rf.nextIndex[i] = nextIndex
		} else {
			rf.nextIndex[i] = reply.XLen
		}
		rf.mu.Unlock()

		if !retry {
			break
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[AppendEntries.start] %d -> %d, request: %+v", args.LeaderId, rf.me, args)

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
	// It can happen only if:
	// - `args.PrevLogIndex` is not in snapshot, since all snaphotted logs are committed and will have the same term as the leader.
	// - Either `args.PrevLogIndex` not in `rf.logs` or its term doesn't match `args.PrevLogTerm`.
	idx := rf.logIdx(args.PrevLogIndex)
	if idx != -1 && (idx >= len(rf.logs) || rf.logs[idx].Term != args.PrevLogTerm) {
		reply.XLen = rf.lastIncludedIndex + 1 + len(rf.logs)
		if idx < len(rf.logs) && rf.logs[idx].Term != args.PrevLogTerm {
			reply.XTerm = rf.logs[idx].Term
			for reply.XIndex = args.PrevLogIndex; reply.XIndex-1 > rf.lastIncludedIndex && rf.logs[rf.logIdx(reply.XIndex-1)].Term == reply.XTerm; reply.XIndex-- {
			}
		}
		DPrintf("[AppendEntries.end] %d -> %d, request: %+v, reply: %+v, log: %v", args.LeaderId, rf.me, args, reply, rf.logs)
		return
	}

	// 3. & 4.
	reply.Success = true
	for i, ent := range args.Entries {
		idx := rf.logIdx(ent.Index)
		if idx == -1 || (idx < len(rf.logs) && rf.logs[idx].Term == ent.Term) {
			// if log entry is already in snapshot or in `rf.logs`, skip it.
			continue
		}
		// 3. If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it.
		if idx < len(rf.logs) && rf.logs[idx].Term != ent.Term {
			rf.logs = rf.logs[:idx]
			rf.logs = append(rf.logs, args.Entries[i:]...)
			rf.persist()
			break
		}
		// 4. Append any new entries NOT ALREADY in the log.
		if idx >= len(rf.logs) {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
	if args.LeaderCommitIndex > rf.commitIndex {
		commitIndex := minInt(args.LeaderCommitIndex, args.PrevLogIndex+len(args.Entries))
		if commitIndex > rf.commitIndex {
			rf.commitIndex = commitIndex
			go rf.startApplyCommit()
		}
	}

	DPrintf("[AppendEntries.end] %d -> %d, request: %+v, reply: %+v, log: %v", args.LeaderId, rf.me, args, reply, rf.logs)
}

// logIdx returns the canonical index of the log entry within `rf.logs` slice.
// If the entry has been trimmed into a snapshot, return -1.
// Notice it doesn't check a too large index, for example, in case where an entry
// hasn't been relicated to a follower.
func (rf *Raft) logIdx(index int) int {
	if index <= rf.lastIncludedIndex {
		return -1
	}
	// index [0, rf.snapshotIndex] is in snapshot.
	return index - rf.lastIncludedIndex - 1
}

func (rf *Raft) lastLogTermIndex() (int, int) {
	if len(rf.logs) == 0 {
		return rf.lastIncludedTerm, rf.lastIncludedIndex
	}
	lastLog := rf.logs[len(rf.logs)-1]
	return lastLog.Term, lastLog.Index
}

func (rf *Raft) prevLogTermIndex(index int) (int, int) {
	idx := rf.logIdx(index)
	if idx == -1 {
		return -1, -1
	}
	if idx == 0 {
		return rf.lastIncludedTerm, rf.lastIncludedIndex
	}
	return rf.logs[idx-1].Term, rf.logs[idx-1].Index
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
	rf.persist()
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
	rf.persist()
}

func (rf *Raft) becomeLeader(term int) {
	DPrintf("[becomeLeader] %d, term: %d -> %d, role: %s -> %s\n", rf.me, rf.currentTerm, term, rf.role, LEADER)
	rf.role = LEADER
	rf.currentTerm = term
	// Doesn't set/reset `votedFor` because to become a leader of `term`, it must
	// have voted for itself.
	_, lastLogIndex := rf.lastLogTermIndex()
	nextIndex := lastLogIndex + 1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = nextIndex
		rf.matchIndex[i] = 0
	}
	go rf.tickerHB()
}

func (rf *Raft) checkTermAndRole(term int, role string) bool {
	return rf.currentTerm == term && rf.role == role
}

func (rf *Raft) startApplyCommit() {
	num := rand.Int63()
	DPrintf("[startApplyCommit.start] %d, num: %d\n", rf.me, num)
	rf.applyCommitCh <- struct{}{}
	DPrintf("[startApplyCommit.end] %d, num: %d\n", rf.me, num)
}

func (rf *Raft) apply() {
	for !rf.killed() {
		select {
		case <-rf.killedCh:
			return
		case <-rf.applyCommitCh:
		}

		// receive apply commit signal, meaning that `rf.commitIndex` has been updated.
		for {
			rf.mu.Lock()
			if rf.lastApplied >= rf.commitIndex {
				rf.mu.Unlock()
				break
			}

			var msg ApplyMsg
			switch {
			case rf.lastApplied < rf.lastIncludedIndex:
				rf.lastApplied = rf.lastIncludedIndex
				msg = ApplyMsg{
					SnapshotValid: true,
					Snapshot:      rf.snapshot,
					SnapshotTerm:  rf.lastIncludedTerm,
					SnapshotIndex: rf.lastIncludedIndex,
				}
			case rf.lastApplied < rf.commitIndex:
				rf.lastApplied++
				idx := rf.logIdx(rf.lastApplied)
				msg = ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[idx].Command,
					CommandIndex: rf.logs[idx].Index,
				}
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
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
	rf.logs = []LogEntry{}
	rf.snapshot = nil
	rf.lastIncludedTerm = 0
	rf.lastIncludedIndex = 0
	rf.role = FOLLOWER

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCommitCh = make(chan struct{})
	rf.killedCh = make(chan struct{}, 1)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply()

	return rf
}
