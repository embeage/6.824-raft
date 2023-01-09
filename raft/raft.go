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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	log          raftLog
	state        int       // Follower, Candidate or Leader
	currentTerm  int       // Term server is in
	nServers     int       // Total number of servers (length of peers)
	voted        bool      // If this server has voted in current term
	votedFor     int       // Holds the id of the server that was voted for 
	votesNeeded  int       // Number of votes needed to determine an election result
	resetElectCh chan bool // Channel to reset election timeout
	stopElectCh  chan bool // Channel to stop election timeout
	stopHeartCh  chan bool // Channel to stop heartbeats
}

func (rf *Raft) isFollower() bool  { return rf.state == Follower }
func (rf *Raft) isCandidate() bool { return rf.state == Candidate }
func (rf *Raft) isLeader() bool    { return rf.state == Leader }

func (rf *Raft) becomeFollower() {
	was := rf.state
	rf.state = Follower
	if was == Leader {
		rf.stopHeartbeats()
		go rf.runElectionTimeout()
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
	go rf.runHeartbeats()
}

func (rf *Raft) updateTerm(newTerm int) {
	if rf.currentTerm != newTerm {
		rf.voted = false
		if rf.currentTerm > newTerm {
			panic("Trying to update to a lower term!")
		}
	}
	rf.currentTerm = newTerm
}

func (rf *Raft) vote(server int) {
	rf.votedFor = server
	rf.voted = true
}

func (rf *Raft) logUpToDate(lastLogIndex, lastLogTerm int) bool {
	if lastLogTerm > rf.log.lastTerm {
		return true
	} else if lastLogTerm == rf.log.lastTerm {
		return lastLogIndex >= rf.log.index
	}
	return false
}

type raftLog struct {
	entries  []RaftLogEntry
	index    int
	lastTerm int
}

type RaftLogEntry struct {
	Command interface{}
	Term    int
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
	// Example:
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// 1. should return immediately
	// 2. send ApplyMsg for each newly committed log entry to the applyCh

	return index, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
// Handle a RequestVote RPC.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm || (rf.isLeader() && args.Term == rf.currentTerm) {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		// Explicitly check if follower so we don't reset election timeout
		if !rf.isFollower() {
			rf.becomeFollower()
		}
	}

	reply.Term = rf.currentTerm

	if !rf.voted || (rf.voted && rf.votedFor == args.CandidateId) {
		if rf.logUpToDate(args.LastLogIndex, args.LastLogTerm) {
			rf.vote(args.CandidateId)
			reply.VoteGranted = true
		}
	}
}

// Handle an AppendEntries RPC.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.becomeFollower()
	} else if args.Term == rf.currentTerm && !rf.isLeader() {
		rf.becomeFollower()
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	rf.state = Follower
	rf.currentTerm = 0
	rf.nServers = len(rf.peers)
	rf.votesNeeded = len(rf.peers)/2 + 1
	rf.resetElectCh = make(chan bool, 1)
	rf.stopElectCh = make(chan bool, 1)
	rf.stopHeartCh = make(chan bool, 1)

	go rf.runElectionTimeout()

	return rf
}

// Return a random duration in ms between startMs and endMs inclusive.
func randDuration(startMs, endMs int) time.Duration {
	r := rand.Intn(endMs-startMs+1) + startMs
	return time.Duration(r) * time.Millisecond
}
