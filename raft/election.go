package raft

import (
	"context"
	"time"
)

const (
	Won = iota
	Lost
	Split
)

type electionResult struct {
	outcome int
	term    int
}

// Run an election. Becomes a candidate, increments the term and votes for self. Then it
// sends out request votes and becomes leader if election is won.
func (rf *Raft) election(ctx context.Context) {
	rf.mu.Lock()

	// Ensure not leader or killed
	if rf.isLeader() || rf.killed() {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm + 1
	rf.becomeCandidate()
	rf.updateTerm(term)
	rf.vote(rf.me)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.index,
		LastLogTerm:  rf.log.lastTerm,
	}

	rf.mu.Unlock()

	replyCh := make(chan *RequestVoteReply, rf.nServers-1)
	rf.sendRequestVotes(replyCh, args)
	ok, result := rf.collectVotes(replyCh, term, ctx)
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
	} else if result.outcome == Won {
		rf.becomeLeader()
	} else if result.outcome == Lost {
		rf.becomeFollower()
	}
	// Stay candidate if none of the above.
}

// Handles the election timeout and elections. The timeout can be stopped by
// sending a value on the stop channel and reset by sending a value on the
// reset channel.
func (rf *Raft) runElectionTimeout() {
	ctx := context.Background()
	var cancelPrev context.CancelFunc
	var electionTimeout *time.Timer

loop:
	for {
		duration := randDuration(400, 700)
		electionTimeout = time.NewTimer(duration)
		ctx, cancel := context.WithCancel(ctx)
		defer electionTimeout.Stop()
		select {
		case <-electionTimeout.C:
			if cancelPrev != nil {
				cancelPrev()
			}
			go rf.election(ctx)
			// Keep a reference to cancel the previous election
			cancelPrev = cancel
		case <-rf.resetElectCh:
			cancel()
		case <-rf.stopElectCh:
			cancel()
			break loop
		}
	}
}

// Resets the election timeout by sending a value on the reset channel.
func (rf *Raft) resetElectionTimeout() {
	select {
	case rf.resetElectCh <- true:
		// Sent reset. Log.
	default:
		// Reset already sent. Do nothing.
	}
}

// Stops the election timeout by sending a value on the stop channel.
func (rf *Raft) stopElectionTimeout() {
	select {
	case rf.stopElectCh <- true:
		// Sent stop. Log.
	default:
		// Stop already sent. Do nothing.
	}
}

// Collect all the available votes on the reply channel. Returns the result of
// the election unless cancelled.
func (rf *Raft) collectVotes(replyCh <-chan *RequestVoteReply, term int, ctx context.Context) (bool, electionResult) {
	votesFor := 1
	votesAgainst := 0

	for i := 0; i < rf.nServers-1; i++ {
		select {
		case reply := <-replyCh:
			// If a server has a higher term, we acknowledge it.
			if reply.Term > term {
				return true, electionResult{OutOfDate, reply.Term}
			}
			if reply.VoteGranted {
				votesFor += 1
			} else {
				votesAgainst += 1
			}
		case <-ctx.Done():
			// This was cancelled due to election timeout.
			return false, electionResult{}
		}
		if votesFor >= rf.votesNeeded {
			return true, electionResult{Won, 0}
		} else if votesAgainst >= rf.votesNeeded {
			return true, electionResult{Lost, 0}
		}
	}
	// Vote split if all votes processed and didn't win or lose.
	return true, electionResult{Split, 0}
}

// Send out request votes asynchronously to all servers except self. Replies are sent to
// the provided reply channel.
func (rf *Raft) sendRequestVotes(replyCh chan<- *RequestVoteReply, args *RequestVoteArgs) {
	for i := 0; i < rf.nServers; i++ {
		if i == rf.me {
			continue
		}
		i := i
		go func() {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)
			if ok {
				replyCh <- reply
			} else {
				// Failed to contact server. Log.
			}
		}()
	}
}

// Send a single request vote to a server.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
