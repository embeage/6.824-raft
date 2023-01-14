package raft

// The states of a peer.
const (
	Follower = iota
	Candidate
	Leader
)

// Outcomes of append entry or election reply processing.
const (
	Done = iota
	OutOfDate
	Cancelled
	Won
	Lost
	Split
)

const (
	// Heartbeats interval in ms.
	HeartbeatsInterval = 50

	// Election timeout range in ms.
	ElectionTimeoutLow  = 300
	ElectionTimeoutHigh = 450
)
