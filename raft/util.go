package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Return a random duration in ms between startMs and endMs inclusive.
func randDuration(startMs, endMs int) time.Duration {
	r := rand.Intn(endMs-startMs+1) + startMs
	return time.Duration(r) * time.Millisecond
}

// Returns the minimum of two integers.
func min(x, y int) int {
	if x <= y {
		return x
	}
	return y
}

// Returns the maximum of two integers.
func max(x, y int) int {
	if x >= y {
		return x
	}
	return y
}
