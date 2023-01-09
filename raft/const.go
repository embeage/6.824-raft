package raft

const (
	Follower = iota
	Candidate
	Leader
)

const (
	Done = iota
	OutOfDate
)
