package metrics

import (
	"sync"
	"time"

	"raftstore/raft"
)

// RaftMetrics contains a snapshot of Raft state.
type RaftMetrics struct {
	NodeID string

	State        string
	CurrentTerm  uint64
	CommitIndex  uint64
	LastLogIndex uint64
	LastApplied  uint64
	LeaderID     string

	LeaderChanges uint64

	FollowerLag map[string]uint64

	LastHeartbeat time.Time
}

// RaftMetricsTracker tracks Raft-level measurements.
type RaftMetricsTracker struct {
	mu sync.Mutex

	leaderChanges uint64
	lastLeaderID  string
}

// NewRaftMetricsTracker creates a tracker.
func NewRaftMetricsTracker() *RaftMetricsTracker {
	return &RaftMetricsTracker{}
}

// RecordLeader observes the current leader and counts changes.
func (t *RaftMetricsTracker) RecordLeader(leaderID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if leaderID == "" {
		return
	}

	if t.lastLeaderID == "" {
		t.lastLeaderID = leaderID
		return
	}

	if t.lastLeaderID != leaderID {
		t.leaderChanges++
		t.lastLeaderID = leaderID
	}
}

// LeaderChanges returns the number of observed leader changes.
func (t *RaftMetricsTracker) LeaderChanges() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.leaderChanges
}

// Snapshot collects the current Raft state.
func (t *RaftMetricsTracker) Snapshot(
	node *raft.Node,
) RaftMetrics {
	if node == nil {
		return RaftMetrics{}
	}

	node.Mu.Lock()
	defer node.Mu.Unlock()

	lastLogIndex := uint64(0)

	if len(node.Log) > 0 {
		lastLogIndex = uint64(len(node.Log) - 1)
	}

	followerLag := make(map[string]uint64)

	for peerID := range node.Peers {
		matchIndex := node.MatchIndex[peerID]

		if lastLogIndex >= matchIndex {
			followerLag[peerID] =
				lastLogIndex - matchIndex
		} else {
			followerLag[peerID] = 0
		}
	}

	t.mu.Lock()

	leaderID := node.LeaderID

	if leaderID != "" &&
		t.lastLeaderID != "" &&
		t.lastLeaderID != leaderID {
		t.leaderChanges++
	}

	if leaderID != "" {
		t.lastLeaderID = leaderID
	}

	leaderChanges := t.leaderChanges

	t.mu.Unlock()

	return RaftMetrics{
		NodeID: node.ID,

		State:         node.State.String(),
		CurrentTerm:   node.CurrentTerm,
		CommitIndex:   node.CommitIndex,
		LastLogIndex:  lastLogIndex,
		LastApplied:   node.LastApplied,
		LeaderID:      leaderID,
		LeaderChanges: leaderChanges,

		FollowerLag: followerLag,

		LastHeartbeat: node.LastHeartbeat,
	}
}
