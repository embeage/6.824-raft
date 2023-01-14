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
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
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

type RaftLogEntry struct {
	Command interface{} // The command of the entry, can be anything.
	Term    int         // Term when the entry was added to the log.
}

type raftLog struct {
	mu       sync.Mutex     // Lock to protect shared access to the log.
	entries  []RaftLogEntry // Entries in the log.
	index    int            // Index of the log, i.e. index of the last entry.
	lastTerm int            // Term of the last entry in the log.
}

// Checks if the log represented by the arguments given is at least
// as up-to-date as our own log.
func (rl *raftLog) upToDate(lastLogIndex, lastLogTerm int) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if lastLogTerm > rl.lastTerm {
		return true
	} else if lastLogTerm == rl.lastTerm {
		return lastLogIndex >= rl.index
	}
	return false
}

// Returns true if the log contains an entry at index.
// Might have to modify when using snapshots to check a range.
func (rl *raftLog) containsEntry(index int) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.index >= index
}

// Returns true if the log contains index and the term of the entry
// at that index matches the provided term. Providing a 0 index is
// deemed as a match since it is used as prevLogIndex when sending
// the first entry of a log.
func (rl *raftLog) match(index, term int) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return index == 0 || (rl.index >= index && rl.entries[index-1].Term == term)
}

// Append an entry to the log, increasing the index by 1.
func (rl *raftLog) appendEntry(command interface{}, term int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.entries = append(rl.entries, RaftLogEntry{command, term})
	rl.lastTerm = term
	rl.index += 1
}

// Get an entry from the log. Uses 1-based indexing although 0-based indexing
// is used internally.
func (rl *raftLog) getEntry(index int) RaftLogEntry {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.entries[index-1]
}

// Get all entries after and including index from.
func (rl *raftLog) getEntries(from int) []RaftLogEntry {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.entries[from-1:]
}

// Remove all entries after and including index from.
func (rl *raftLog) removeEntries(from int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	before := len(rl.entries)
	rl.entries = rl.entries[:from-1]
	removed := before - len(rl.entries)
	rl.index -= removed
	if from != 1 {
		rl.lastTerm = rl.entries[from-2].Term
	} else {
		rl.lastTerm = 0
	}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state.
	peers         []*labrpc.ClientEnd // RPC end points of all peers.
	nPeers        int                 // Total number of peers.
	majority      int                 // Majority number of nPeers.
	persister     *Persister          // Object to hold this peer's persisted state.
	me            int                 // This peer's index into peers[].
	dead          int32               // Set by Kill().
	log           raftLog             // The log of this peer.
	state         int                 // The state the peer is in. Follower, Candidate or Leader.
	currentTerm   int                 // The term the peer is in.
	voted         bool                // If this server has voted in current term.
	votedFor      int                 // Server that was voted for if voted is true.
	commitIndex   int                 // Index of highest log entry known to be committed.
	lastApplied   int                 // Index of highest log entry applied to state machine.
	nextIndex     []int               // Index of the next log entry to send to each peer.
	matchIndex    []int               // Index of the highest log entry known to be replicated on each peer.
	applyCh       chan ApplyMsg       // Channel to apply entries to state machine.
	applyCommitCh chan int            // Channel to send newly committed entries on to get applied.
	stopApplierCh chan struct{}       // Channel to signal stop applier.
	resetElectCh  chan struct{}       // Channel to signal reset election timeout.
	stopElectCh   chan struct{}       // Channel to signal stop election timeout.
	stopHeartCh   chan struct{}       // Channel to signal stop heartbeats.
}

func (rf *Raft) incrementFollower(server, replicatedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[server] = replicatedIndex + 1
	rf.matchIndex[server] = replicatedIndex
}

func (rf *Raft) decrementFollower(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.nextIndex[server] != 1 {
		rf.nextIndex[server] -= 1
	}
}

// Commits (sets commitIndex = N) if there exists an N such that a
// majority of matchIndex[i] >= N and log[N].term == currentTerm.
// Also ensures that the highest possible N is committed.
// This is called by the leader.
func (rf *Raft) maybeCommit() {

	// Ensure leader and not killed
	if !rf.isLeader() || rf.killed() {
		return
	}

	var N int

	// Loop backwards to ensure highest possible N.
	for i := rf.log.index; i > rf.commitIndex; i-- {
		count := 1
		for _, match := range rf.matchIndex {
			if match >= i {
				count++
			}
		}
		if count >= rf.majority && rf.log.getEntry(i).Term == rf.currentTerm {
			N = i
			break
		}
	}

	// Only commit if N was assigned.
	if N != 0 {
		rf.commitIndex = N
		rf.applyCommitCh <- N
	}
}

// Update to leader commit if leader commit is larger than our own.
func (rf *Raft) maybeUpdateCommit(leaderCommit int) {
	if leaderCommit > rf.commitIndex {
		if leaderCommit <= rf.log.index {
			rf.commitIndex = leaderCommit
		} else {
			rf.commitIndex = rf.log.index
		}
		rf.commitIndex = leaderCommit
		rf.applyCommitCh <- leaderCommit
	}
}

func (rf *Raft) isFollower() bool {
	return rf.state == Follower
}

func (rf *Raft) isLeader() bool {
	return rf.state == Leader
}

func (rf *Raft) maybeBecomeFollower() {
	was := rf.state
	rf.state = Follower
	if was == Leader {
		rf.stopHeartbeats()
		go rf.runElectionTimeout()
		// If we're a candidate or already a follower, just reset election timeout.
	} else {
		rf.resetElectionTimeout()
	}
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
}

func (rf *Raft) becomeLeader() {
	rf.stopElectionTimeout()
	rf.state = Leader
	for i := 0; i < rf.nPeers; i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.log.index + 1
		rf.matchIndex[i] = 0
	}
	go rf.runHeartbeats()
}

func (rf *Raft) maybeUpdateTerm(term int) {
	if rf.currentTerm != term {
		rf.voted = false
		rf.currentTerm = term
	}
}

func (rf *Raft) vote(server int) {
	rf.votedFor = server
	rf.voted = true
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.isLeader()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isLeader() && !rf.killed() {
		rf.log.appendEntry(command, rf.currentTerm)
	}

	return rf.log.index, rf.currentTerm, rf.isLeader()
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
	rf.stopHeartbeats()
	rf.stopElectionTimeout()
	rf.stopApplier()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Handle a RequestVote RPC.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Sender is not up to date.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.maybeUpdateTerm(args.Term)
		// Check if follower so we don't reset election timeout otherwise
		if !rf.isFollower() {
			rf.maybeBecomeFollower()
		}
	}

	reply.Term = rf.currentTerm

	if !rf.voted || (rf.voted && rf.votedFor == args.CandidateId) {
		if rf.log.upToDate(args.LastLogIndex, args.LastLogTerm) {
			rf.vote(args.CandidateId)
			reply.VoteGranted = true
		}
	}
}

// Handle an AppendEntries RPC.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Sender is not up to date.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term >= rf.currentTerm {
		rf.maybeUpdateTerm(args.Term)
		rf.maybeBecomeFollower()
	}

	reply.Term = rf.currentTerm

	// Overwrite log with leader's log if the logs match. args.Entries
	// will be empty if it's a heartbeat.
	if rf.log.match(args.PrevLogIndex, args.PrevLogTerm) {
		for i, entry := range args.Entries {
			i := args.PrevLogIndex + i + 1

			// If existing entry conflicts with new one, remove it and following entries.
			if rf.log.containsEntry(i) && !rf.log.match(i, entry.Term) {
				rf.log.removeEntries(i)
			}

			// Append new entry if it's not already in the log.
			if !rf.log.containsEntry(i) {
				rf.log.appendEntry(entry.Command, entry.Term)
			}
		}
		reply.Success = true

		// Update commit if the leader's commit is higher than ours.
		rf.maybeUpdateCommit(args.LeaderCommit)
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
	rf.nPeers = len(rf.peers)
	rf.majority = len(rf.peers)/2 + 1
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, rf.nPeers)
	rf.matchIndex = make([]int, rf.nPeers)
	rf.applyCh = applyCh
	rf.applyCommitCh = make(chan int, 10)
	rf.resetElectCh = make(chan struct{}, 1)
	rf.stopElectCh = make(chan struct{}, 1)
	rf.stopHeartCh = make(chan struct{}, 1)

	// Run long-running Goroutines. Applier should run through
	// the course of the peer, while ElectionTimeout will be
	// switched with Heartbeats when becoming to leader.
	go rf.runApplier()
	go rf.runElectionTimeout()

	return rf
}
