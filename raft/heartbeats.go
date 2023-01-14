package raft

import (
	"context"
	"time"
)

type appendEntriesResult struct {
	// Outcome of processing all append entries replies.
	// Done, OutOfDate or Cancelled.
	outcome int

	// Populated if the outcome is OutOfDate.
	term int
}

type serverAppendEntriesReply struct {
	// Server associated with reply.
	server int

	// Index of the last entry replicated by server if
	// append entries was successful.
	replicatedIndex int

	// The RPC reply.
	reply *AppendEntriesReply
}

// Handles sending out heartbeats to all other servers and commits
// if possible.
func (rf *Raft) heartbeats(ctx context.Context) {
	rf.mu.Lock()

	// Ensure leader and not killed
	if !rf.isLeader() || rf.killed() {
		rf.mu.Unlock()
		return
	}

	replyCh := make(chan serverAppendEntriesReply, rf.nPeers-1)
	rf.sendHeartbeats(replyCh)

	term := rf.currentTerm
	rf.mu.Unlock()

	// Await replies until all servers responded or cancelled.
	result := rf.gatherAppendEntriesReplies(replyCh, term, ctx)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch result.outcome {
	case Done, Cancelled:
		// Try to commit in case majority replied with success.
		rf.maybeCommit()
	case OutOfDate:
		if result.term > rf.currentTerm {
			rf.maybeUpdateTerm(result.term)
			rf.maybeBecomeFollower()
		}
	}
}

// Handles sending heartbeats periodically. Can be stopped by sending a value
// on the stop channel. When a new heartbeats are sent previous ones will be
// cancelled if they are still running.
func (rf *Raft) runHeartbeats() {
	ctx := context.Background()
	var cancelPrev context.CancelFunc
	heartbeatTicker := time.NewTicker(HeartbeatsInterval * time.Millisecond)
	defer heartbeatTicker.Stop()

loop:
	for {
		ctx, cancel := context.WithCancel(ctx)
		if cancelPrev != nil {
			cancelPrev()
		}
		go rf.heartbeats(ctx)
		cancelPrev = cancel
		select {
		case <-heartbeatTicker.C:
			continue
		case <-rf.stopHeartCh:
			if cancelPrev != nil {
				cancelPrev()
			}
			cancel()
			break loop
		}
	}
}

// Stops the periodic heartbeats by sending a value on the stop channel.
func (rf *Raft) stopHeartbeats() {
	select {
	case rf.stopHeartCh <- struct{}{}:
		// Sent stop. Log.
	default:
		// Stop already sent. Do nothing.
	}
}

// Collects available replies from heartbeats on the reply channel.
func (rf *Raft) gatherAppendEntriesReplies(replyCh <-chan serverAppendEntriesReply, term int, ctx context.Context) appendEntriesResult {
	for i := 0; i < rf.nPeers-1; i++ {
		select {
		case serverReply := <-replyCh:
			if serverReply.reply.Term > term {
				return appendEntriesResult{outcome: OutOfDate, term: serverReply.reply.Term}
			}
			if serverReply.reply.Success {
				rf.incrementFollower(serverReply.server, serverReply.replicatedIndex)
			} else {
				rf.decrementFollower(serverReply.server)
			}
		case <-ctx.Done():
			// Cancelled due to new heartbeats being sent.
			return appendEntriesResult{outcome: Cancelled}
		}
	}
	return appendEntriesResult{outcome: Done}
}

// Send out heartbeats asynchronously to all servers except self. Replies are sent to
// the provided reply channel. Entries are included with heartbeats if the nextIndex
// of the follower is lower or equal to our own log.
func (rf *Raft) sendHeartbeats(replyCh chan<- serverAppendEntriesReply) {
	for i := 0; i < rf.nPeers; i++ {
		if i == rf.me {
			continue
		}

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

		nextIndex := rf.nextIndex[i]
		// Send all entries after and including nextIndex to follower.
		if rf.log.index >= nextIndex {
			args.PrevLogIndex = nextIndex - 1
			if nextIndex != 1 {
				args.PrevLogTerm = rf.log.getEntry(args.PrevLogIndex).Term
			}
			entries := rf.log.getEntries(nextIndex)
			args.Entries = make([]RaftLogEntry, len(entries))
			copy(args.Entries, entries)
			// No entries to send, "empty" heartbeat.
		} else {
			args.PrevLogIndex = rf.log.index
			args.PrevLogTerm = rf.log.lastTerm
		}

		server := i
		index := rf.log.index
		go func() {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, reply)
			if ok {
				replyCh <- serverAppendEntriesReply{server, index, reply}
			} else {
				// Failed to contact. Log.
			}
		}()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
