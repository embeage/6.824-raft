package raft

// Apply all committed entries to the state machine.
func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.commitIndex > rf.lastApplied && !rf.killed() {
		rf.lastApplied += 1
		command := rf.command(rf.lastApplied)
		commandIndex := rf.lastApplied
		rf.mu.Unlock()
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: commandIndex,
		}
		rf.mu.Lock()
	}
}

func (rf *Raft) signalApply() {
	select {
	case rf.applyCommitCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) runApplier() {
	for {
		select {
		case <-rf.applyCommitCh:
			rf.applyEntries()
		case <-rf.stopApplierCh:
			close(rf.applyCh)
			return
		}
	}
}

func (rf *Raft) stopApplier() {
	select {
	case rf.stopApplierCh <- struct{}{}:
	default:
	}
}
