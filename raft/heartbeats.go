package raft

import (
	"time"
)

type appendEntriesResult struct {
	// Server associated with reply.
	server int

	// Term when the RPC was sent.
	sentTerm int

	// The RPC reply.
	reply *AppendEntriesReply

	// Index of the last entry replicated by server if
	// AppendEntries RPC was successful.
	newIndex int

	// Set if the RPC was successful (i.e. not dropped).
	success bool
}

// Sends out AppendEntries RPC's to all other servers and handles the results.
func (rf *Raft) appendEntries() {
	rf.mu.Lock()

	if !rf.isLeader() || rf.killed() {
		rf.mu.Unlock()
		return
	}

	resultCh := make(chan appendEntriesResult, len(rf.peers)-1)
	rf.sendHeartbeats(resultCh)

	// Handle all replies/results.
	for i := 0; i < len(rf.peers)-1; i++ {
		if !rf.isLeader() || rf.killed() {
			break
		}
		rf.mu.Unlock()
		result := <-resultCh
		rf.mu.Lock()
		rf.handleAppendEntriesResult(result)
	}
	rf.mu.Unlock()
}

// Handles sending heartbeats periodically. Can be stopped by sending a value
// on the stop channel.
func (rf *Raft) runHeartbeats() {
	heartbeatTicker := time.NewTicker(HeartbeatsInterval * time.Millisecond)
	defer heartbeatTicker.Stop()
	for {
		go rf.appendEntries()
		select {
		case <-heartbeatTicker.C:
			continue
		case <-rf.stopHeartCh:
			return
		}
	}
}

// Stops the periodic heartbeats by sending a value on the stop channel.
func (rf *Raft) stopHeartbeats() {
	select {
	case rf.stopHeartCh <- struct{}{}:
	default:
	}
}

// Handles a single heartbeat result.
func (rf *Raft) handleAppendEntriesResult(result appendEntriesResult) {
	// RPC reply failed.
	if !result.success {
		return
	}

	// Our term is outdated.
	if result.reply.Term > rf.currentTerm {
		rf.increaseTerm(result.reply.Term)
		rf.becomeFollower()
		return
	}

	// The reply term is different than the one sent in the RPC.
	if result.reply.Term != result.sentTerm {
		return
	}

	server := result.server

	// We have successfully replicated newIndex in the server.
	if result.reply.Success {
		rf.nextIndex[server] = max(rf.nextIndex[server], result.newIndex+1)
		rf.matchIndex[server] = max(rf.matchIndex[server], result.newIndex)
		// Try to commit.
		go rf.maybeCommit()
		return
	}

	// Our try to replicate newIndex was unsuccessful, update nextIndex
	// using the conflicting index and term.
	conflictTerm := result.reply.ConflictTerm
	conflictIndex := result.reply.ConflictIndex

	if conflictTerm != None {
		lastIndex := rf.lastIndexForTerm(conflictTerm)
		if lastIndex != None {
			rf.nextIndex[server] = lastIndex + 1
		} else {
			rf.nextIndex[server] = conflictIndex
		}
		return
	}

	// Follower's log is too short.
	rf.nextIndex[server] = conflictIndex + 1
}

// Send out heartbeats asynchronously to all servers except self. Results
// are sent on the provided result channel.
func (rf *Raft) sendHeartbeats(resultCh chan<- appendEntriesResult) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		server := i
		sentTerm := rf.currentTerm

		nextIndex := rf.nextIndex[server]
		prevLogIndex := nextIndex - 1
		prevLogTerm := rf.term(prevLogIndex)
		var entries []LogEntry

		if lastIndex, _ := rf.lastIndexAndTerm(); lastIndex >= nextIndex {
			newEntries := rf.entriesFrom(nextIndex)
			entries = make([]LogEntry, len(newEntries))
			copy(entries, newEntries)
		}

		newIndex := prevLogIndex + len(entries)

		args := &AppendEntriesArgs{
			Term:         sentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		go func() {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, reply)
			resultCh <- appendEntriesResult{server, sentTerm, reply, newIndex, ok}
		}()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
