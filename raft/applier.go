package raft

// Apply all committed entries to the state machine.
func (rf *Raft) applyEntries(commitIndex int) {
	for commitIndex > rf.lastApplied {
		rf.lastApplied += 1
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log.getEntry(rf.lastApplied).Command,
			CommandIndex: rf.lastApplied,
		}
	}
}

func (rf *Raft) runApplier() {
loop:
	for {
		select {
		case commitIndex := <-rf.applyCommitCh:
			rf.applyEntries(commitIndex)
		case <-rf.stopApplierCh:
			break loop
		}
	}
}

func (rf *Raft) stopApplier() {
	select {
	case rf.stopApplierCh <- struct{}{}:
	default:
	}
}
