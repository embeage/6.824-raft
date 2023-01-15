package raft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"6.824/labgob"
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

type LogEntry struct {
	Index   int         // Index of the log entry.
	Term    int         // Term when the entry was added to the log.
	Command interface{} // The command of the entry, can be anything.
}

// Checks if the log represented by the arguments given is at least
// as up-to-date as our own log.
func (rf *Raft) logUpToDate(lastLogIndex, lastLogTerm int) bool {
	lastIndex, lastTerm := rf.lastIndexAndTerm()
	return lastLogTerm > lastTerm || lastLogTerm == lastTerm && lastLogIndex >= lastIndex
}

// Returns the index and term of the last entry in the log.
func (rf *Raft) lastIndexAndTerm() (int, int) {
	if len(rf.log) == 0 {
		return 0, None
	}
	return rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term
}

// Append an entry to the log.
func (rf *Raft) appendEntry(command interface{}, term int) {
	prevIndex, _ := rf.lastIndexAndTerm()
	index := prevIndex + 1
	rf.log = append(rf.log, LogEntry{index, term, command})
}

// Get the term of a log entry.
func (rf *Raft) term(index int) int {
	if lastIndex, _ := rf.lastIndexAndTerm(); lastIndex >= index && index > 0 {
		return rf.log[index-1].Term
	}
	return None
}

// Get the command of a log entry.
func (rf *Raft) command(index int) interface{} {
	if lastIndex, _ := rf.lastIndexAndTerm(); lastIndex >= index && index > 0 {
		return rf.log[index-1].Command
	}
	return None
}

// Get all entries from (and including) index.
func (rf *Raft) entriesFrom(index int) []LogEntry {
	return rf.log[index-1:]
}

// Remove all entries from (and including) index.
func (rf *Raft) removeEntriesFrom(index int) {
	rf.log = rf.log[:index-1]
}

func (rf *Raft) firstIndexForTerm(term int) int {
	lastIndex, _ := rf.lastIndexAndTerm()
	for i := 1; i <= lastIndex; i++ {
		if rf.term(i) == term {
			return i
		}
	}
	return None
}

func (rf *Raft) lastIndexForTerm(term int) int {
	lastIndex, _ := rf.lastIndexAndTerm()
	for i := lastIndex; i > 0; i-- {
		if rf.term(i) == term {
			return i
		}
	}
	return None
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state.
	peers         []*labrpc.ClientEnd // RPC end points of all peers.
	persister     *Persister          // Object to hold this peer's persisted state.
	me            int                 // This peer's index into peers[].
	dead          int32               // Set by Kill().
	log           []LogEntry          // The log of this peer.
	state         int                 // The state the peer is in. Follower, Candidate or Leader.
	currentTerm   int                 // The term the peer is in.
	votedFor      int                 // Server that was voted for this term.
	commitIndex   int                 // Index of highest log entry known to be committed.
	lastApplied   int                 // Index of highest log entry applied to state machine.
	nextIndex     []int               // Index of the next log entry to send to each peer.
	matchIndex    []int               // Index of the highest log entry known to be replicated on each peer.
	applyCh       chan ApplyMsg       // Channel to apply entries to state machine.
	applyCommitCh chan struct{}       // Channel to send signal that we should apply.
	stopApplierCh chan struct{}       // Channel to signal stop applier.
	resetElectCh  chan struct{}       // Channel to signal reset election timeout.
	stopElectCh   chan struct{}       // Channel to signal stop election timeout.
	stopHeartCh   chan struct{}       // Channel to signal stop heartbeats.
}

// Commits (sets commitIndex = N) if there exists an N such that a
// majority of matchIndex[i] >= N and log[N].term == currentTerm.
// Also ensures that the highest possible N is committed.
func (rf *Raft) maybeCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() || rf.killed() {
		return
	}

	// Loop backwards to ensure highest possible N.
	lastIndex, _ := rf.lastIndexAndTerm()
	for n := lastIndex; n > rf.commitIndex; n-- {
		if rf.term(n) != rf.currentTerm {
			continue
		}
		cnt := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= n {
				cnt++
			}
		}
		if cnt >= len(rf.peers)/2+1 {
			rf.commitIndex = n
			rf.signalApply()
			return
		}
	}
}

func (rf *Raft) isLeader() bool {
	return rf.state == Leader
}

func (rf *Raft) isCandidate() bool {
	return rf.state == Candidate
}

func (rf *Raft) becomeFollower() {
	if rf.state == Leader {
		rf.stopHeartbeats()
		go rf.runElectionTimeout()
	}
	rf.state = Follower
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
}

func (rf *Raft) becomeLeader() {
	rf.stopElectionTimeout()
	lastIndex, _ := rf.lastIndexAndTerm()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = None
	}
	rf.state = Leader
	go rf.runHeartbeats()
}

func (rf *Raft) increaseTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = None
	rf.persist()
}

func (rf *Raft) vote(server int) {
	rf.votedFor = server
	rf.persist()
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var log []LogEntry
	var votedFor int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&votedFor) != nil {
		panic("Couldn't read persisted state!")
	} else {
		rf.currentTerm = currentTerm
		rf.log = log
		rf.votedFor = votedFor
	}
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
		rf.appendEntry(command, rf.currentTerm)
		rf.persist()
	}

	lastIndex, _ := rf.lastIndexAndTerm()
	return lastIndex, rf.currentTerm, rf.isLeader()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Stop the long-running goroutines
	if rf.isLeader() {
		rf.stopHeartbeats()
	} else {
		rf.stopElectionTimeout()
	}
	rf.stopApplier()

	close(rf.applyCommitCh)
	close(rf.stopApplierCh)
	close(rf.resetElectCh)
	close(rf.stopElectCh)
	close(rf.stopHeartCh)
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
	if args.Term < rf.currentTerm || rf.killed() {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.increaseTerm(args.Term)
		rf.becomeFollower()
	}

	reply.Term = rf.currentTerm

	if (rf.votedFor == None || rf.votedFor == args.CandidateId) && rf.logUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.vote(args.CandidateId)
		rf.resetElectionTimeout()
	}
}

// Handle an AppendEntries RPC.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Sender is not up to date.
	if args.Term < rf.currentTerm || rf.killed() {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.increaseTerm(args.Term)
		rf.becomeFollower()
	}

	reply.Term = rf.currentTerm

	if args.Term == rf.currentTerm && rf.isCandidate() {
		rf.becomeFollower()
	}

	rf.resetElectionTimeout()

	// Log is too short.
	if lastIndex, _ := rf.lastIndexAndTerm(); lastIndex < args.PrevLogIndex {
		reply.ConflictTerm = None
		reply.ConflictIndex = lastIndex
		return
	}

	// Log doesn't contain an entry at PrevLogIndex whose term matches PrevLogTerm.
	if term := rf.term(args.PrevLogIndex); term != args.PrevLogTerm {
		reply.ConflictTerm = term
		reply.ConflictIndex = rf.firstIndexForTerm(term)
		return
	}

	logChanged := false
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
	
		// Existing entry conflicts with new one, remove it and following entries.
		if lastIndex, _ := rf.lastIndexAndTerm(); lastIndex >= index && rf.term(index) != entry.Term {
			rf.removeEntriesFrom(index)
		}

		// Append new entry if it's not already in the log.
		if lastIndex, _ := rf.lastIndexAndTerm(); lastIndex < index {
			rf.appendEntry(entry.Command, entry.Term)
			logChanged = true
		}
	}

	if logChanged {
		rf.persist()
	}

	reply.Success = true

	// Update commit if the leader's commit is higher than ours.
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, lastNewIndex)
		rf.signalApply()
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
	rf.log = []LogEntry{}
	rf.state = Follower
	rf.votedFor = None
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	rf.applyCommitCh = make(chan struct{}, 1)
	rf.stopApplierCh = make(chan struct{}, 1)
	rf.resetElectCh = make(chan struct{}, 1)
	rf.stopElectCh = make(chan struct{}, 1)
	rf.stopHeartCh = make(chan struct{}, 1)

	rf.readPersist(persister.ReadRaftState())

	// Run long-running Goroutines. Applier should run through
	// the course of the peer, while ElectionTimeout will be
	// switched with Heartbeats when becoming to leader.
	go rf.runApplier()
	go rf.runElectionTimeout()

	return rf
}
