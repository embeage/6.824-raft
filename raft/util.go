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
