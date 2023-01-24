package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// The states of a peer.
	follower  = 0
	candidate = 1
	leader    = 2

	// Heartbeats interval in ms.
	heartbeatsInterval = 100

	// Election timeout range in ms.
	electionTimeoutLow  = 800
	electionTimeoutHigh = 1200

	// Representation of a none value e.g. voted for no one.
	none = -1
)

type ApplyMsg struct {
	// For applying entries.
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For applying snapshots.
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

type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state.
	peers             []*labrpc.ClientEnd // RPC end points of all peers.
	persister         *Persister          // Object to hold this peer's persisted state.
	me                int                 // This peer's index into peers[].
	dead              int32               // Set by Kill().
	log               []LogEntry          // The log of this peer.
	state             int                 // The state the peer is in. Follower, Candidate or Leader.
	currentTerm       int                 // The term the peer is in.
	votedFor          int                 // Server that was voted for this term.
	commitIndex       int                 // Index of highest log entry known to be committed.
	lastApplied       int                 // Index of highest log entry applied to state machine.
	lastIncludedIndex int                 // The last included index of the latest snapshot.
	lastIncludedTerm  int                 // The last included term of the latest snapshot.
	appendEntriesNo   int                 // Number of times append entries have been sent out.
	nextIndex         []int               // Index of the next log entry to send to each peer.
	matchIndex        []int               // Index of the highest log entry known to be replicated on each peer.
	applyCh           chan ApplyMsg       // Channel to apply entries or snapshot to state machine.
	snapshotCh        chan snapshot       // Channel to send incoming snapshots on to be applied.
	applyCommitCh     chan struct{}       // Channel to signal that we should apply up to commit index.
	stopApplierCh     chan struct{}       // Channel to signal stop applier.
	resetElectCh      chan struct{}       // Channel to signal reset election timeout.
	stopElectCh       chan struct{}       // Channel to signal stop election timeout.
	stopHeartCh       chan struct{}       // Channel to signal stop heartbeats.
}

/************************ STATE ************************/

// Returns currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.isLeader()
}

// If this server believes it is a candidate.
func (rf *Raft) isCandidate() bool {
	return rf.state == candidate
}

// If this server believes it is the leader.
func (rf *Raft) isLeader() bool {
	return rf.state == leader
}

// Step down to a follower.
func (rf *Raft) becomeFollower() {
	if rf.state == leader {
		rf.stopHeartbeats()
		go rf.electionTimeout()
	}
	rf.state = follower
}

// Convert to a candidate when starting an election.
func (rf *Raft) becomeCandidate() {
	rf.state = candidate
}

// Convert to a leader (after winning an election).
func (rf *Raft) becomeLeader() {
	rf.stopElectionTimeout()
	lastLogIndex := rf.lastLogIndex()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.state = leader
	go rf.heartbeats()
}

// Updates the term and resets votedFor.
func (rf *Raft) updateTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = none
	rf.persist(nil)
}

// Votes for a server.
func (rf *Raft) vote(server int) {
	rf.votedFor = server
	rf.persist(nil)
}

// Commits if there exists an N such that a majority of matchIndex[i] >= N and
// log[N].term == term. Ensures that the highest possible N is committed.
func (rf *Raft) tryCommit(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() || rf.currentTerm != term || rf.killed() {
		return
	}

	// Go backwards to ensure highest possible N.
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.term(n) != term {
			continue
		}
		cnt := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= n {
				cnt++
			}
		}
		if cnt >= len(rf.peers)/2+1 {
			// Commit successful.
			rf.commitIndex = n
			rf.applyEntries()
			return
		}
	}
}

/************************ LOG ************************/

// Returns the index of the last entry in the log. If the log
// is empty it returns the last included log index.
func (rf *Raft) lastLogIndex() int {
	if l := len(rf.log); l != 0 {
		return rf.log[l-1].Index
	}
	return rf.lastIncludedIndex
}

// Returns the term of the last entry in the log. If the log
// is empty it returns the last included term.
func (rf *Raft) lastLogTerm() int {
	if l := len(rf.log); l != 0 {
		return rf.log[l-1].Term
	}
	return rf.lastIncludedTerm
}

// Get an entry of the log if present, else nil.
func (rf *Raft) entry(index int) *LogEntry {
	if rf.lastIncludedIndex < index && index <= rf.lastLogIndex() {
		return &rf.log[index-rf.lastIncludedIndex-1]
	}
	return nil
}

// Get the term of a log entry if present, else 0.
func (rf *Raft) term(index int) int {
	if entry := rf.entry(index); entry != nil {
		return entry.Term
	}
	return 0
}

// Get the command of a log entry if present, else nil.
func (rf *Raft) command(index int) interface{} {
	if entry := rf.entry(index); entry != nil {
		return entry.Command
	}
	return nil
}

// Append an entry to the log.
func (rf *Raft) appendEntry(command interface{}, term int) {
	index := rf.lastLogIndex() + 1
	rf.log = append(rf.log, LogEntry{index, term, command})
}

// Get all entries from (and including) index.
func (rf *Raft) entryAndFollowing(index int) []LogEntry {
	return rf.log[index-rf.lastIncludedIndex-1:]
}

// Remove all entries from (and including) index.
func (rf *Raft) removeEntryAndFollowing(index int) {
	rf.log = rf.log[:index-rf.lastIncludedIndex-1]
}

// Remove all entries before index (and including) index.
func (rf *Raft) removeEntryAndPrevious(index int) {
	rf.log = rf.log[index-rf.lastIncludedIndex:]
}

// Get the index of the first entry with the given term if present, else 0.
func (rf *Raft) firstLogIndexForTerm(term int) int {
	for i := rf.lastIncludedIndex + 1; i <= rf.lastLogIndex(); i++ {
		if rf.term(i) == term {
			return i
		}
	}
	return 0
}

// Get the index of the last entry with the given term if present, else 0.
func (rf *Raft) lastLogIndexForTerm(term int) int {
	for i := rf.lastLogIndex(); i > rf.lastIncludedIndex; i-- {
		if rf.term(i) == term {
			return i
		}
	}
	return 0
}

// Checks if our log is more up-to-date than the log represented by the arguments.
func (rf *Raft) ourLogMoreUpToDate(lastLogIndex, lastLogTerm int) bool {
	ourLastLogIndex, ourLastLogTerm := rf.lastLogIndex(), rf.lastLogTerm()
	if ourLastLogTerm != lastLogTerm {
		return ourLastLogTerm > lastLogTerm
	}
	return ourLastLogIndex > lastLogIndex
}

/************************ ELECTIONS & REQUEST VOTES ************************/

type RequestVoteArgs struct {
	Term         int // Candidate's term.
	CandidateId  int // Candidate requesting vote.
	LastLogIndex int // Index of candidate's last log entry.
	LastLogTerm  int // Term of candidate's last log entry.
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself.
	VoteGranted bool // true means candidate received vote.
}

type requestVoteResult struct {
	server  int               // Server associated with reply.
	success bool              // If the RPC was successful.
	reply   *RequestVoteReply // The RPC reply.
}

// Starts election after the election timeout has passed unless reset or stopped.
func (rf *Raft) electionTimeout() {
	for {
		duration := durationBetween(electionTimeoutLow, electionTimeoutHigh)
		electionTimeout := time.NewTimer(duration)
		select {
		case <-electionTimeout.C:
			go rf.election()
		case <-rf.resetElectCh:
			electionTimeout.Stop()
		case <-rf.stopElectCh:
			electionTimeout.Stop()
			return
		}
	}
}

// Triggers reset of the election timeout.
func (rf *Raft) resetElectionTimeout() {
	select {
	case rf.resetElectCh <- struct{}{}:
	default:
	}
}

// Triggers stop of the election timeout.
func (rf *Raft) stopElectionTimeout() {
	select {
	case rf.stopElectCh <- struct{}{}:
	default:
	}
}

// Become a candidate and run an election. Sends out RequestVote RPC's to
// all other servers and handles the results.
func (rf *Raft) election() {
	rf.mu.Lock()

	if rf.isLeader() || rf.killed() {
		rf.mu.Unlock()
		return
	}

	rf.becomeCandidate()
	rf.updateTerm(rf.currentTerm + 1)
	rf.vote(rf.me)

	term := rf.currentTerm
	resultCh := make(chan requestVoteResult, len(rf.peers)-1)
	rf.sendRequestVotes(resultCh)

	votes := 1
	for i := 0; i < len(rf.peers)-1; i++ {
		if !rf.isCandidate() || rf.killed() {
			break
		}
		rf.mu.Unlock()
		result := <-resultCh
		rf.mu.Lock()
		rf.handleRequestVoteResult(result, term, &votes)
	}
	rf.mu.Unlock()
}

// Handles a request vote result.
func (rf *Raft) handleRequestVoteResult(result requestVoteResult, term int, votes *int) {
	// RPC reply failed and retries cancelled.
	if !result.success {
		return
	}

	// Our term is outdated.
	if result.reply.Term > rf.currentTerm {
		rf.updateTerm(result.reply.Term)
		rf.becomeFollower()
		return
	}

	// Our current term is different than the one sent in the RPC.
	if rf.currentTerm != term {
		return
	}

	if !result.reply.VoteGranted {
		return
	}

	// Vote received.
	*votes += 1

	// Check if won election.
	if *votes >= len(rf.peers)/2+1 {
		rf.becomeLeader()
	}
}

// Send out request votes asynchronously to all servers except self. Results
// are sent on the the provided result channel.
func (rf *Raft) sendRequestVotes(resultCh chan<- requestVoteResult) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		server := i
		term := rf.currentTerm

		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.lastLogIndex(),
			LastLogTerm:  rf.lastLogTerm(),
		}

		go func() {
			reply := &RequestVoteReply{}
			success := false
			rf.mu.Lock()
			// Retry RPC until success or we start another election (term changes).
			for !success && rf.currentTerm == term && rf.isCandidate() && !rf.killed() {
				rf.mu.Unlock()
				success = rf.sendRequestVoteRPC(server, args, reply)
				time.Sleep(1 * time.Millisecond)
				rf.mu.Lock()
			}
			rf.mu.Unlock()
			resultCh <- requestVoteResult{server, success, reply}
		}()
	}
}

// Send a single request vote to a server.
func (rf *Raft) sendRequestVoteRPC(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Handle a RequestVote RPC.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Candidate is not up-to-date.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// Our term is outdated.
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.becomeFollower()
	}

	reply.Term = rf.currentTerm

	if (rf.votedFor == none || rf.votedFor == args.CandidateId) && !rf.ourLogMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.vote(args.CandidateId)
		rf.resetElectionTimeout()
	}
}

/************************ HEARTBEATS & APPEND ENTRIES ************************/

type AppendEntriesArgs struct {
	Term         int        // Leader's term.
	LeaderId     int        // Leader's id.
	PrevLogIndex int        // Index of log entry immediately preceding new ones.
	PrevLogTerm  int        // Term of PrevLogIndex.
	Entries      []LogEntry // Entries to store (empty for heartbeat).
	LeaderCommit int        // Leader's commitIndex.
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself.
	Success bool // true if follower contained entry matching PrevLogIndex/PrevLogTerm.

	// Log backtracking optimization.
	ConflictTerm  int // The conflicting term if there is one.
	ConflictIndex int // The conflicting index if there is one.
}

type appendEntriesResult struct {
	server   int                 // Server associated with reply.
	success  bool                // If the RPC was successful.
	reply    *AppendEntriesReply // The RPC reply.
	newIndex int                 // Index of the replicated entry if successful.
}

// Sends out append entries immediately and then periodically until stopped.
func (rf *Raft) heartbeats() {
	pulse := time.NewTicker(heartbeatsInterval * time.Millisecond)
	for {
		go rf.appendEntries()
		select {
		case <-pulse.C:
			continue
		case <-rf.stopHeartCh:
			pulse.Stop()
			return
		}
	}
}

// Triggers stop of the periodic heartbeats.
func (rf *Raft) stopHeartbeats() {
	select {
	case rf.stopHeartCh <- struct{}{}:
	default:
	}
}

// Sends out AppendEntries RPC's to all other servers and handles the results.
func (rf *Raft) appendEntries() {
	rf.mu.Lock()

	if !rf.isLeader() || rf.killed() {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm
	rf.appendEntriesNo += 1
	resultCh := make(chan appendEntriesResult, len(rf.peers)-1)
	rf.sendAppendEntries(resultCh)

	// Handle all replies/results.
	for i := 0; i < len(rf.peers)-1; i++ {
		if !rf.isLeader() || rf.killed() {
			break
		}
		rf.mu.Unlock()
		result := <-resultCh
		rf.mu.Lock()
		rf.handleAppendEntriesResult(result, term)
	}
	rf.mu.Unlock()
}

// Handles an append entries result.
func (rf *Raft) handleAppendEntriesResult(result appendEntriesResult, term int) {
	// RPC reply failed and retries cancelled or had to install snapshot.
	if !result.success {
		return
	}

	// Our term is outdated.
	if result.reply.Term > rf.currentTerm {
		rf.updateTerm(result.reply.Term)
		rf.becomeFollower()
		return
	}

	// Our current term is different than the one sent in the RPC.
	if rf.currentTerm != term {
		return
	}

	server := result.server

	// We successfully replicated newIndex in the follower.
	if result.reply.Success {
		prevMatchIndex := rf.matchIndex[server]
		rf.nextIndex[server] = max(rf.nextIndex[server], result.newIndex+1)
		rf.matchIndex[server] = max(rf.matchIndex[server], result.newIndex)

		// Try to commit if matchIndex increases.
		if rf.matchIndex[server] > prevMatchIndex {
			go rf.tryCommit(rf.currentTerm)
		}
		return
	}

	// Our try to replicate newIndex was unsuccessful, update nextIndex
	// using the conflicting index and term.
	conflictTerm := result.reply.ConflictTerm
	conflictIndex := result.reply.ConflictIndex

	// Follower has PrevLogIndex in its log, but there's a conflict.
	if conflictTerm != 0 {

		// Find last entry in the log with ConflictTerm.
		if lastForTerm := rf.lastLogIndexForTerm(conflictTerm); lastForTerm != 0 {
			rf.nextIndex[server] = lastForTerm + 1
			return
		}

		// We did not find ConflictTerm in our log.
		rf.nextIndex[server] = conflictIndex
		return
	}

	// Follower's log is too short.
	rf.nextIndex[server] = conflictIndex + 1
}

// Send append entries to all servers except self. Results are sent
// on the provided result channel.
func (rf *Raft) sendAppendEntries(resultCh chan<- appendEntriesResult) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		server := i
		appendEntriesNo := rf.appendEntriesNo

		nextIndex := rf.nextIndex[server]
		prevLogIndex := nextIndex - 1
		prevLogTerm := rf.term(prevLogIndex)
		var entries []LogEntry

		// Check that nextIndex falls between lastIncludedIndex and lastLogIndex.
		if rf.lastIncludedIndex < nextIndex && nextIndex <= rf.lastLogIndex() {
			newEntries := rf.entryAndFollowing(nextIndex)
			// Copy to a new slice to avoid race in RPC.
			entries = make([]LogEntry, len(newEntries))
			copy(entries, newEntries)
			// We have discarded nextIndex due to a snapshot, need to install.
		} else if nextIndex <= rf.lastIncludedIndex {
			go rf.installSnapshot(server)
			resultCh <- appendEntriesResult{server: server, success: false}
			continue
		}

		newIndex := prevLogIndex + len(entries)

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		go func() {
			reply := &AppendEntriesReply{}
			success := false
			rf.mu.Lock()
			// Retry RPC until success or until we start another round of append entries.
			for !success && rf.appendEntriesNo == appendEntriesNo && rf.isLeader() && !rf.killed() {
				rf.mu.Unlock()
				success = rf.sendAppendEntriesRPC(server, args, reply)
				time.Sleep(1 * time.Millisecond)
				rf.mu.Lock()
			}
			rf.mu.Unlock()
			resultCh <- appendEntriesResult{server, success, reply, newIndex}
		}()
	}
}

// Send a single append entries RPC.
func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Handle an AppendEntries RPC.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Leader is not up-to-date.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// Our term is outdated.
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.becomeFollower()
	}

	// If candidate, accept new leader in current term.
	if args.Term == rf.currentTerm && rf.isCandidate() {
		rf.becomeFollower()
	}

	rf.resetElectionTimeout()
	reply.Term = rf.currentTerm

	// Our log is too short.
	if lastLogIndex := rf.lastLogIndex(); lastLogIndex < args.PrevLogIndex {
		reply.ConflictIndex = lastLogIndex
		return
	}

	// Our log doesn't contain an entry at PrevLogIndex whose term matches PrevLogTerm.
	if term := rf.term(args.PrevLogIndex); term != args.PrevLogTerm {
		reply.ConflictTerm = term
		reply.ConflictIndex = rf.firstLogIndexForTerm(term)
		return
	}

	logChanged := false
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1

		// We have discarded this due to a newer snapshot.
		if index <= rf.lastIncludedIndex {
			continue
		}

		// Existing entry in our log conflicts with new one, remove it and following entries.
		if index <= rf.lastLogIndex() && rf.term(index) != entry.Term {
			rf.removeEntryAndFollowing(index)
		}

		// Append new entry if it's not already in our log.
		if rf.lastLogIndex() < index {
			rf.appendEntry(entry.Command, entry.Term)
			logChanged = true
		}
	}

	if logChanged {
		rf.persist(nil)
	}

	reply.Success = true

	// Update commit if the leader's commit is higher than ours.
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, lastNewIndex)
		rf.applyEntries()
	}
}

/************************ LOG COMPACTION & INSTALL SNAPSHOTS ************************/

type InstallSnapshotArgs struct {
	Term              int    // Leader's term.
	LeaderId          int    // Leader's id.
	LastIncludedIndex int    // Snapshot replaces all entries up through and including this index.
	LastIncludedTerm  int    // Term of LastIncludedIndex.
	Data              []byte // Raw bytes of the snapshot chunk.
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself.
}

type snapshot struct {
	data  []byte // Raw bytes of the snapshot.
	index int    // Index of last included entry.
	term  int    // Term of last included entry.
}

// The service has confirmed the snapshot on the apply channel. Only install if
// Raft hasn't got more recent info since it communicated the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Don't install if we have a newer snapshot or we have applied the entries in the snapshot already.
	if lastIncludedIndex <= rf.lastIncludedIndex || lastIncludedIndex <= rf.lastApplied {
		return false
	}

	install := func() {
		// Update state to match snapshot.
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.persist(snapshot)
	}

	// If existing log entry has the same index and term as snapshot's
	// last included entry, discard it and previous entries.
	for i := rf.lastIncludedIndex + 1; i <= rf.lastLogIndex(); i++ {
		if i == lastIncludedIndex && rf.term(i) == lastIncludedTerm {
			rf.removeEntryAndPrevious(i)
			install()
			return true
		}
	}

	// Else discard the entire log.
	rf.log = []LogEntry{}
	install()
	return true
}

// The service says it has created a snapshot that has
// all info up to and including index. This means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastIncludedTerm = rf.term(index)
	rf.removeEntryAndPrevious(index)
	rf.lastIncludedIndex = index
	rf.persist(snapshot)
}

func (rf *Raft) installSnapshot(server int) {
	rf.mu.Lock()

	if !rf.isLeader() || rf.killed() {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm
	appendEntriesNo := rf.appendEntriesNo
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}

	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}
	success := false
	rf.mu.Lock()
	// Retry RPC until success or until we start another round of append entries.
	for !success && rf.appendEntriesNo == appendEntriesNo && rf.isLeader() && !rf.killed() {
		rf.mu.Unlock()
		success = rf.sendInstallSnapshotRPC(server, args, reply)
		time.Sleep(1 * time.Millisecond)
		rf.mu.Lock()
	}

	defer rf.mu.Unlock()

	// RPC reply failed and retries cancelled.
	if !success {
		return
	}

	// Our term is outdated.
	if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
		rf.becomeFollower()
		return
	}

	// Our current term is different than the one sent in the RPC.
	if rf.currentTerm != term {
		return
	}

	// Assume installed by setting updating nextIndex and matchIndex.
	// If it failed this will resolve itself by log backtracking.
	rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex+1)
	rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)

}

// Send a single install snapshot RPC.
func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// Handle a InstallSnapshot RPC.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Leader is not up-to-date.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// Our term is outdated.
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.becomeFollower()
	}

	rf.resetElectionTimeout()
	reply.Term = rf.currentTerm

	// No need to send snapshot to applier if ours is newer or we have applied the entries in the snapshot already.
	if args.LastIncludedIndex <= rf.lastIncludedIndex || args.LastIncludedIndex <= rf.lastApplied {
		return
	}

	// Add to snapshot channel to be applied by applier when it is ready.
	// Buffered a bit (can be increased) to be non-blocking.
	rf.snapshotCh <- snapshot{
		data:  args.Data,
		index: args.LastIncludedIndex,
		term:  args.LastIncludedTerm,
	}
}

/************************ APPLIER ************************/

// Applies to the state machine when it receives a value on
// the apply commit channel or the snapshot channel.
func (rf *Raft) applier() {
	for {
		select {
		case <-rf.applyCommitCh:
			rf.mu.Lock()
			for rf.commitIndex > rf.lastApplied && !rf.killed() {
				rf.lastApplied += 1
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.command(rf.lastApplied),
					CommandIndex: rf.lastApplied,
				}
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
			}
			rf.mu.Unlock()
		case snapshot := <-rf.snapshotCh:
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshot.data,
				SnapshotTerm:  snapshot.term,
				SnapshotIndex: snapshot.index,
			}
		case <-rf.stopApplierCh:
			close(rf.applyCh)
			return
		}
	}
}

// Triggers apply of entries up to commit index.
func (rf *Raft) applyEntries() {
	select {
	case rf.applyCommitCh <- struct{}{}:
	default:
	}
}

// Triggers stop of the applier.
func (rf *Raft) stopApplier() {
	select {
	case rf.stopApplierCh <- struct{}{}:
	default:
	}
}

/************************ PERSISTANCE ************************/

// Save Raft's persistent state and snapshot to stable storage. Only saves
// state if snapshot is nil.
func (rf *Raft) persist(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	if snapshot != nil {
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	} else {
		rf.persister.SaveRaftState(data)
	}
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var log []LogEntry
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("Couldn't read persisted state!")
	} else {
		rf.currentTerm = currentTerm
		rf.log = log
		rf.votedFor = votedFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		// Fast-forward commitIndex and lastApplied to the latest snapshot.
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}

/************************ CLEANUP ************************/

// Stops long-running goroutines and closes channels.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isLeader() {
		rf.stopHeartbeats()
	} else {
		rf.stopElectionTimeout()
	}
	rf.stopApplier()

	close(rf.applyCommitCh)
	close(rf.snapshotCh)
	close(rf.stopApplierCh)
	close(rf.resetElectCh)
	close(rf.stopElectCh)
	close(rf.stopHeartCh)
}

// If this Raft server has been killed.
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/************************ AGREEMENT ************************/

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log.
// Returns false if this server isn't the leader. Otherwise,
// starts the agreement and returns immediately.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isLeader() && !rf.killed() {
		rf.appendEntry(command, rf.currentTerm)
		rf.persist(nil)
		// Optional to send append entries right away. Will be
		// sent on the next pulse.
		// go rf.appendEntries()
	}

	return rf.lastLogIndex(), rf.currentTerm, rf.isLeader()
}

/************************ CREATE ************************/

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. Persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		log:               []LogEntry{},
		state:             follower,
		currentTerm:       0,
		votedFor:          none,
		commitIndex:       0,
		lastApplied:       0,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		appendEntriesNo:   0,
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		applyCh:           applyCh,
		snapshotCh:        make(chan snapshot, 10),
		applyCommitCh:     make(chan struct{}, 1),
		stopApplierCh:     make(chan struct{}, 1),
		resetElectCh:      make(chan struct{}, 1),
		stopElectCh:       make(chan struct{}, 1),
		stopHeartCh:       make(chan struct{}, 1),
	}

	// Restore persisted state if any.
	rf.readPersist(persister.ReadRaftState())

	// Start long-running goroutines. applier() should run through
	// the course of the peer, while electionTimeout() will be
	// switched with heartbeats() when becoming to leader.
	go rf.applier()
	go rf.electionTimeout()

	return rf
}
