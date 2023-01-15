package raft

import (
	"time"
)

type requestVoteResult struct {
	// Server associated with reply.
	server int

	// Term when the RPC was sent
	sentTerm int

	// The RPC reply.
	reply *RequestVoteReply

	// Set if the RPC was successful (i.e. not dropped).
	success bool
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
	rf.increaseTerm(rf.currentTerm + 1)
	rf.vote(rf.me)

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
		rf.handleRequestVoteResult(result, &votes)
	}
	rf.mu.Unlock()
}

// Handles the election timeout and runs elections. Can be stopped by
// sending a value on the stop channel and reset by sending a
// value on the reset channel.
func (rf *Raft) runElectionTimeout() {
	for {
		duration := randDuration(ElectionTimeoutLow, ElectionTimeoutHigh)
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

// Resets the election timeout by sending a value on the reset channel.
func (rf *Raft) resetElectionTimeout() {
	select {
	case rf.resetElectCh <- struct{}{}:
	default:
	}
}

// Stops the election timeout by sending a value on the stop channel.
func (rf *Raft) stopElectionTimeout() {
	select {
	case rf.stopElectCh <- struct{}{}:
	default:
	}
}

// Handles a single request vote result.
func (rf *Raft) handleRequestVoteResult(result requestVoteResult, votes *int) {
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
		sentTerm := rf.currentTerm
		lastIndex, lastTerm := rf.lastIndexAndTerm()

		args := &RequestVoteArgs{
			Term:         sentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		}

		go func() {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			resultCh <- requestVoteResult{server, sentTerm, reply, ok}
		}()
	}
}

// Send a single request vote to a server.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
