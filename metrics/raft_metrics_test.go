package metrics

import (
	"testing"
	"time"

	"raftstore/raft"
)

func TestRaftMetricsSnapshot(t *testing.T) {
	node := &raft.Node{
		ID:          "node1",
		State:       raft.Leader,
		CurrentTerm: 5,

		Log: []raft.LogEntry{
			{
				Term:    0,
				Command: nil,
			},
			{
				Term:    5,
				Command: []byte("command1"),
			},
			{
				Term:    5,
				Command: []byte("command2"),
			},
			{
				Term:    5,
				Command: []byte("command3"),
			},
			{
				Term:    5,
				Command: []byte("command4"),
			},
			{
				Term:    5,
				Command: []byte("command5"),
			},
		},

		CommitIndex: 4,
		LastApplied: 4,

		Peers: map[string]string{
			"node2": "localhost:5002",
			"node3": "localhost:5003",
		},

		MatchIndex: map[string]uint64{
			"node2": 4,
			"node3": 2,
		},

		LeaderID:      "node1",
		LastHeartbeat: time.Now(),
	}

	tracker := NewRaftMetricsTracker()

	stats := tracker.Snapshot(node)

	if stats.NodeID != "node1" {
		t.Fatalf(
			"expected node1, got %s",
			stats.NodeID,
		)
	}

	if stats.State != "Leader" {
		t.Fatalf(
			"expected Leader, got %s",
			stats.State,
		)
	}

	if stats.CurrentTerm != 5 {
		t.Fatalf(
			"expected term 5, got %d",
			stats.CurrentTerm,
		)
	}

	if stats.CommitIndex != 4 {
		t.Fatalf(
			"expected commit index 4, got %d",
			stats.CommitIndex,
		)
	}

	if stats.LastLogIndex != 5 {
		t.Fatalf(
			"expected last log index 5, got %d",
			stats.LastLogIndex,
		)
	}

	if stats.LastApplied != 4 {
		t.Fatalf(
			"expected last applied 4, got %d",
			stats.LastApplied,
		)
	}

	if stats.LeaderID != "node1" {
		t.Fatalf(
			"expected leader node1, got %s",
			stats.LeaderID,
		)
	}

	if stats.FollowerLag["node2"] != 1 {
		t.Fatalf(
			"expected node2 lag 1, got %d",
			stats.FollowerLag["node2"],
		)
	}

	if stats.FollowerLag["node3"] != 3 {
		t.Fatalf(
			"expected node3 lag 3, got %d",
			stats.FollowerLag["node3"],
		)
	}
}
