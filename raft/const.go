package raft

const (
	// The states of a peer.
	Follower = 0
	Candidate = 1
	Leader = 2

	// Heartbeats interval in ms.
	HeartbeatsInterval = 100

	// Election timeout range in ms.
	ElectionTimeoutLow  = 800
	ElectionTimeoutHigh = 1200

	// Representation of a none value e.g. no one voted for.
	None = -1
)
