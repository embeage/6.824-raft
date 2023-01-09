package raft

import (
	"context"
	"time"
)

type heartbeatResult struct {
	outcome int
	term    int
}

// Handles sending out heartbeats to all other servers.
func (rf *Raft) heartbeats(ctx context.Context) {
	rf.mu.Lock()

	// Ensure leader and not killed
	if !rf.isLeader() || rf.killed() {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm
	args := &AppendEntriesArgs{
		Term: rf.currentTerm,
	}

	rf.mu.Unlock()

	replyCh := make(chan *AppendEntriesReply, rf.nServers-1)
	rf.sendHeartbeats(replyCh, args)
	ok, result := rf.collectHeartbeatsReplies(replyCh, term, ctx)
	if !ok {
		// Cancelled
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if result.outcome == OutOfDate && result.term > rf.currentTerm {
		// Need to update its log. Might happen automatically.
		rf.updateTerm(result.term)
		rf.becomeFollower()
	}
}

// Handles sending heartbeats periodically. Can be stopped by sending a value
// on the stop channel.
func (rf *Raft) runHeartbeats() {
	ctx := context.Background()
	var cancelPrev context.CancelFunc
	heartbeatTicker := time.NewTicker(200 * time.Millisecond)
	defer heartbeatTicker.Stop()

loop:
	for {
		ctx, cancel := context.WithCancel(ctx)
		if cancelPrev != nil {
			cancelPrev()
		}
		go rf.heartbeats(ctx)
		// Keep a reference to cancel the previous heartbeats
		cancelPrev = cancel

		select {
		case <-heartbeatTicker.C:
			continue
		case <-rf.stopHeartCh:
			cancel()
			break loop
		}
	}
}

// Stops the periodic heartbeats by sending a value on the stop channel.
func (rf *Raft) stopHeartbeats() {
	select {
	case rf.stopHeartCh <- true:
		// Sent stop. Log.
	default:
		// Stop already sent. Do nothing.
	}
}

// Collects available replies from heartbeats on the reply channel. They are used
// to check if the term is up to date.
func (rf *Raft) collectHeartbeatsReplies(replyCh <-chan *AppendEntriesReply, term int, ctx context.Context) (bool, heartbeatResult) {
	for i := 0; i < rf.nServers-1; i++ {
		select {
		case reply := <-replyCh:
			if reply.Term > term {
				return true, heartbeatResult{OutOfDate, term}
			}
		case <-ctx.Done():
			// Cancelled due to more heartbeats being sent.
			return false, heartbeatResult{}
		}
	}
	return true, heartbeatResult{Done, 0}
}

// Send out heartbeats asynchronously to all servers except self. Replies are sent to 
// the provided reply channel.
func (rf *Raft) sendHeartbeats(replyCh chan<- *AppendEntriesReply, args *AppendEntriesArgs) {
	for i := 0; i < rf.nServers; i++ {
		if i == rf.me {
			continue
		}

		i := i
		go func() {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, args, reply)
			if ok {
				replyCh <- reply
			} else {
				// Failed to contact. Log.
			}
		}()
	}
}
