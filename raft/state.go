package raft

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    uint64
	Command []byte
}
