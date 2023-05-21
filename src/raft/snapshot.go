package raft

import (
	"fmt"
	"time"
)

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[Snapshot.start] %d, index: %d, lastIncludedIndex: %d, log length: %d", rf.me, index, rf.lastIncludedIndex, len(rf.logs))
	// If the snapshot is older than the current snapshot, ignore it.
	if index <= rf.lastIncludedIndex {
		return
	}
	if index > rf.lastApplied || index > rf.commitIndex {
		panic(fmt.Sprintf("Snapshot shouldn't be created from an index (%d) larger than lastApplied or commitIndex (%d, %d)",
			index, rf.lastApplied, rf.commitIndex))
	}

	idx := rf.logIdx(index)
	rf.snapshot = snapshot
	rf.lastIncludedTerm = rf.logs[idx].Term
	rf.lastIncludedIndex = rf.logs[idx].Index
	rf.logs = rf.logs[idx+1:]
	rf.persist()
	DPrintf("[Snapshot.end] %d, index: %d, lastIncludedIndex: %d, log length: %d", rf.me, index, rf.lastIncludedIndex, len(rf.logs))
}

func (rf *Raft) sendInstallSnapshotToOne(i int) {
	rf.mu.Lock()
	request := InstallSnapshotArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		if !rf.checkTermAndRole(request.Term, LEADER) {
			rf.mu.Unlock()
			return
		}
		if rf.matchIndex[i] >= rf.lastIncludedIndex {
			rf.mu.Unlock()
			return
		}

		request.LastIncludedTerm = rf.lastIncludedTerm
		request.LastIncludedIndex = rf.lastIncludedIndex
		request.Data = rf.snapshot
		reply := InstallSnapshotReply{}
		rf.mu.Unlock()

		ok := rf.sendInstallSnapshot(i, &request, &reply)
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
		if !ok {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		rf.matchIndex[i] = maxInt(rf.matchIndex[i], request.LastIncludedIndex)
		rf.nextIndex[i] = rf.matchIndex[i] + 1
		rf.mu.Unlock()
	}
}

// sendInstallSnapshot is called by leader to send snapshot to follower. When leader
// sends log entries (`AppendEntries` RPC) and finds that follower's `nextIndex` is
// smaller than the index of its first log entry, it means the follower's log is
// too old and requires a snapshot. In this case, leader will send a snapshot insead.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// InstallSnapshot is the RPC handler for follower to receive and install a snapshot
// from leader.
//
// When a Raft server receives an snapshot, the following may happen, assuming the snapshot is indeed from leader:
// 1. the snapshot is older than existing one: ignore the request.
// 2. the snapshot is newer than existing one, but older compared to `lastApplied`: ignore the request.
// 3. the snapshot is newer than existing one and `lastApplied`, but older than `commitIndex`: update snapshot and logs, apply.
// 4. the snapshot is newer than all logs: update snapshot and remove all logs, apply.
// 5. the snapshot's lastIncludedTerm and lastIncludedIndex conflict with existing logs: same as 4.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[InstallSnapshot.start] %d -> %d, snapshot term: %d, snapshot index: %d", args.LeaderId, rf.me, args.LastIncludedTerm, args.LastIncludedIndex)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.receiveHB = true
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// nothing to process if
	// (1). the snapshot is older than the current snapshot.
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	// (2). the snapshot didn't provide any new information known to state machine.
	if args.LastIncludedIndex <= rf.lastApplied {
		return
	}

	// The new snapshot has new data than previous snapshot.
	// pre-calculate the idx of `LastIncludedIndex` in the current "snapshot + logs"
	// before they're overwritten. Used in step 6.
	oldIdx := rf.logIdx(args.LastIncludedIndex)

	// 5. Save snapshot, discard any existing or partial snapshot with a smaller index
	if rf.lastIncludedIndex < args.LastIncludedIndex {
		rf.snapshot = args.Data
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.lastIncludedIndex = args.LastIncludedIndex
	}

	if oldIdx != -1 && oldIdx < len(rf.logs) && rf.logs[oldIdx].Term == args.LastIncludedTerm {
		// 6. If existing (applied) log entry has same index and term as snapshot’s last included entry,
		// retain log entries following it
		rf.logs = rf.logs[oldIdx+1:]
	} else {
		// 7. Discard the entire log
		rf.logs = []LogEntry{}
	}

	// 8. Reset state machine using snapshot contents
	rf.persist()
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
		go rf.startApplyCommit()
	}

	DPrintf("[InstallSnapshot.end] %d -> %d, snapshot term: %d, snapshot index: %d", args.LeaderId, rf.me, args.LastIncludedTerm, args.LastIncludedIndex)
}
