package raft

import (
	"math/rand"
	"sync"
	"time"
)

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

		go func() {
			var reply RequestVoteReply
			ok := false
			for !rf.killed() {
				reply = RequestVoteReply{}
				if ok = rf.sendRequestVote(i, &request, &reply); ok {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
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
	DPrintf("[sendRequestVote.start] %d -> %d, request: %+v", args.CandidateId, server, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("[sendRequestVote.end] %d -> %d, request: %+v, reply: %+v, result: %t", args.CandidateId, server, args, reply, ok)
	return ok
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("[RequestVote.start] %d -> %d (votedFor: %d), request: %+v, reply: %+v", args.CandidateId, rf.me, rf.votedFor, args, reply)
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
		rf.persist()
		reply.VoteGranted = true
		// Reset election timer: grant vote to candidate.
		rf.receiveHB = true
	}

	DPrintf("[RequestVote.end] %d -> %d (votedFor: %d), request: %+v, reply: %+v", args.CandidateId, rf.me, rf.votedFor, args, reply)
}
