package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "raftstore/proto"
	"raftstore/storage"
)

type Node struct {
	Mu sync.Mutex

	ID          string
	State       State
	CurrentTerm uint64
	VotedFor    string

	Log         []LogEntry
	CommitIndex uint64
	LastApplied uint64

	Peers   map[string]string
	Clients map[string]pb.RaftServiceClient

	NextIndex  map[string]uint64
	MatchIndex map[string]uint64

	VotesReceived int

	ElectionReset chan bool
	StopChan      chan bool

	LastHeartbeat time.Time
	LeaderID      string

	// Separated persistent storage.
	//
	// RocksDB:
	//     key -> ValuePointer(offset, length)
	//
	// ValueLog:
	//     actual value bytes
	KV *storage.SeparatedKVStore
}

// NewNode creates a new Raft node.
func NewNode(id string, peers map[string]string) *Node {
	nodeDataDir := filepath.Join("data", id)

	// Create the node data directory.
	if err := os.MkdirAll(nodeDataDir, 0755); err != nil {
		panic(fmt.Errorf(
			"failed to create data directory for node %s: %w",
			id,
			err,
		))
	}

	// Open separated storage.
	//
	// This creates/opens:
	//
	// data/<node-id>/rocksdb/
	// data/<node-id>/values.log
	//
	kv, err := storage.NewSeparatedKVStore(nodeDataDir)
	if err != nil {
		panic(fmt.Errorf(
			"failed to open separated storage for node %s: %w",
			id,
			err,
		))
	}

	node := &Node{
		ID:          id,
		State:       Follower,
		CurrentTerm: 0,
		VotedFor:    "",

		// Dummy entry at index 0.
		Log: []LogEntry{
			{
				Term:    0,
				Command: nil,
			},
		},

		CommitIndex: 0,
		LastApplied: 0,

		Peers:   peers,
		Clients: make(map[string]pb.RaftServiceClient),

		NextIndex:  make(map[string]uint64),
		MatchIndex: make(map[string]uint64),

		VotesReceived: 0,

		ElectionReset: make(chan bool, 1),
		StopChan:      make(chan bool),

		LastHeartbeat: time.Now(),
		LeaderID:      "",

		KV: kv,
	}

	// Restore persistent Raft state.
	//
	// loadPersistentState() is implemented in
	// raft/persistence.go.
	if err := node.loadPersistentState(); err != nil {
		node.KV.Close()

		panic(fmt.Errorf(
			"failed to load persistent state for node %s: %w",
			id,
			err,
		))
	}

	// Rebuild the state machine from committed log entries.
	node.replayCommittedLog()

	return node
}

// replayCommittedLog rebuilds the KV state from committed Raft entries.
//
// The Raft log remains the source of truth for committed operations.
// During startup, committed commands are replayed into the
// SeparatedKVStore.
func (n *Node) replayCommittedLog() {
	if n.CommitIndex == 0 {
		n.LastApplied = 0
		return
	}

	lastLogIndex := uint64(len(n.Log) - 1)

	// Protect against an invalid persisted commit index.
	if n.CommitIndex > lastLogIndex {
		n.CommitIndex = lastLogIndex
	}

	for n.LastApplied < n.CommitIndex {
		n.LastApplied++

		if n.LastApplied >= uint64(len(n.Log)) {
			break
		}

		entry := n.Log[n.LastApplied]

		command, err := decodeCommand(entry.Command)
		if err != nil {
			fmt.Printf(
				"Node %s failed to decode log index %d during replay: %v\n",
				n.ID,
				n.LastApplied,
				err,
			)

			continue
		}

		switch command.Operation {

		case "PUT":
			if err := n.KV.Put(
				command.Key,
				command.Value,
			); err != nil {
				panic(fmt.Errorf(
					"failed to replay PUT for key %q: %w",
					command.Key,
					err,
				))
			}

		case "DELETE":
			if err := n.KV.Delete(
				command.Key,
			); err != nil {
				panic(fmt.Errorf(
					"failed to replay DELETE for key %q: %w",
					command.Key,
					err,
				))
			}
		}
	}
}
