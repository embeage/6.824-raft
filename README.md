## MIT 6.824 Raft

Implementation of the Raft consensus algorithm (see [the extended Raft paper](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)). Based on the MIT 6.824 Distributed Systems lab. Runs by the way of the tester, `go test`.

Implements the core parts of the paper, as well as snapshotting. Also includes a few optimizations such as the accelerated log backtracking described on page 7-8. The Raft code is divided into clear sections reflecting parts of the paper and is commented thoroughly. It features an easy to understand design, with two long-running goroutines alive simultaneously during the lifetime of a node. The applier goroutine will run through the course of the node, while the election timeout goroutine will be swapped with the heartbeats goroutine when becoming leader and vice-versa when stepping down. AppendEntries are sent out on each heartbeat, but can optionally be sent on each appended log entry to the leader if desired. This will reduce the real time performance at the cost of CPU time.

[Test results (500 iterations)](https://github.com/embeage/6.824-raft/blob/master/tests.txt)
- [x] 2A Leader Election
- [x] 2B Log
- [x] 2C Persistance
- [x] 2D Log compaction
