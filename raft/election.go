package raft

import (
	"context"
	"time"
)

type electionResult struct {
	// Outcome of processing all request vote replies.
	// Won, Lost, Split, OutOfDate or Cancelled.
	outcome int

	// Populated if the outcome is OutOfDate.
	term int
}

type serverRequestVoteReply struct {
	// Server associated with reply.
	server int

	// The RPC reply.
	reply *RequestVoteReply
}

// Run an election. Become a candidate, increment the term and vote for self.
// Send out request votes and become leader if election is won.
func (rf *Raft) election(ctx context.Context) {
	rf.mu.Lock()

	// Ensure not leader or killed
	if rf.isLeader() || rf.killed() {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm + 1
	rf.becomeCandidate()
	rf.maybeUpdateTerm(term)
	rf.vote(rf.me)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.index,
		LastLogTerm:  rf.log.lastTerm,
	}

	replyCh := make(chan serverRequestVoteReply, rf.nPeers-1)
	rf.sendRequestVotes(replyCh, args)
	rf.mu.Unlock()

	// Await replies until all servers responded or cancelled.
	result := rf.gatherRequestVoteReplies(replyCh, term, ctx)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch result.outcome {
	case Won:
		rf.becomeLeader()
	case Lost:
		rf.maybeBecomeFollower()
	case Split, Cancelled:
	case OutOfDate:
		if result.term > rf.currentTerm {
			rf.maybeUpdateTerm(result.term)
			rf.maybeBecomeFollower()
		}
	}
}

// Handles the election timeout and elections. The timeout can be stopped by
// sending a value on the stop channel and reset by sending a value on the
// reset channel. When a new election is run the previous one will be cancelled
// if it is still running.
func (rf *Raft) runElectionTimeout() {
	ctx := context.Background()
	var cancelPrev context.CancelFunc
	var electionTimeout *time.Timer

loop:
	for {
		duration := randDuration(ElectionTimeoutLow, ElectionTimeoutHigh)
		electionTimeout = time.NewTimer(duration)
		ctx, cancel := context.WithCancel(ctx)
		defer electionTimeout.Stop()
		select {
		case <-electionTimeout.C:
			if cancelPrev != nil {
				cancelPrev()
			}
			go rf.election(ctx)
			cancelPrev = cancel
		case <-rf.resetElectCh:
			cancel()
		case <-rf.stopElectCh:
			if cancelPrev != nil {
				cancelPrev()
			}
			cancel()
			break loop
		}
	}
}

// Resets the election timeout by sending a value on the reset channel.
func (rf *Raft) resetElectionTimeout() {
	select {
	case rf.resetElectCh <- struct{}{}:
		// Sent reset. Log.
	default:
		// Reset already sent. Do nothing.
	}
}

// Stops the election timeout by sending a value on the stop channel.
func (rf *Raft) stopElectionTimeout() {
	select {
	case rf.stopElectCh <- struct{}{}:
		// Sent stop. Log.
	default:
		// Stop already sent. Do nothing.
	}
}

// Gather all the available votes on the reply channel. Returns the result of
// the election unless cancelled.
func (rf *Raft) gatherRequestVoteReplies(replyCh <-chan serverRequestVoteReply, term int, ctx context.Context) electionResult {
	votesFor := 1
	votesAgainst := 0

	for i := 0; i < rf.nPeers-1; i++ {
		select {
		case serverReply := <-replyCh:
			if serverReply.reply.Term > term {
				return electionResult{outcome: OutOfDate, term: serverReply.reply.Term}
			}
			if serverReply.reply.VoteGranted {
				votesFor += 1
			} else {
				votesAgainst += 1
			}
		case <-ctx.Done():
			// Cancelled due to election timeout.
			return electionResult{outcome: Cancelled}
		}
		if votesFor >= rf.majority {
			return electionResult{outcome: Won}
		} else if votesAgainst >= rf.majority {
			return electionResult{outcome: Lost}
		}
	}
	// Vote must be split if all votes processed and didn't win or lose.
	return electionResult{outcome: Split}
}

// Send out request votes asynchronously to all servers except self. Replies are sent to
// the provided reply channel.
func (rf *Raft) sendRequestVotes(replyCh chan<- serverRequestVoteReply, args *RequestVoteArgs) {
	for i := 0; i < rf.nPeers; i++ {
		if i == rf.me {
			continue
		}
		server := i
		go func() {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				replyCh <- serverRequestVoteReply{server, reply}
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
