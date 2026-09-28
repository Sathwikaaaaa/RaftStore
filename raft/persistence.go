package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// PersistentState contains the Raft state that must survive
// a node restart.
//
// CurrentTerm, VotedFor, and Log are core Raft persistent state.
// CommitIndex is also persisted so that a restarted node knows
// which portion of its log was known to be committed.
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    string
	Log         []LogEntry
	CommitIndex uint64
}

// savePersistentState writes the current Raft state to disk.
//
// IMPORTANT:
// The caller must hold n.Mu.
func (n *Node) savePersistentState() error {

	state := PersistentState{
		CurrentTerm: n.CurrentTerm,
		VotedFor:    n.VotedFor,
		Log:         cloneLog(n.Log),
		CommitIndex: n.CommitIndex,
	}

	data, err := json.MarshalIndent(
		state,
		"",
		"  ",
	)

	if err != nil {
		return err
	}

	// Each node gets its own persistent directory.
	dir := filepath.Join(
		"data",
		n.ID,
	)

	if err := os.MkdirAll(
		dir,
		0755,
	); err != nil {
		return err
	}

	statePath := filepath.Join(
		dir,
		"raft_state.json",
	)

	tempPath := filepath.Join(
		dir,
		"raft_state.tmp",
	)

	// Write to a temporary file first.
	// This prevents a partially-written state file from
	// replacing the previous valid state.
	if err := os.WriteFile(
		tempPath,
		data,
		0644,
	); err != nil {
		return err
	}

	// Windows does not replace an existing file using
	// os.Rename in the same way Unix systems do.
	// Remove the previous state file first.
	if err := os.Remove(statePath); err != nil &&
		!os.IsNotExist(err) {

		_ = os.Remove(tempPath)

		return err
	}

	if err := os.Rename(
		tempPath,
		statePath,
	); err != nil {

		_ = os.Remove(tempPath)

		return err
	}

	return nil
}

// loadPersistentState restores persistent Raft state.
//
// If no state file exists, the node starts with its
// normal initial state.
func (n *Node) loadPersistentState() error {

	statePath := filepath.Join(
		"data",
		n.ID,
		"raft_state.json",
	)

	data, err := os.ReadFile(statePath)

	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return err
	}

	var state PersistentState

	if err := json.Unmarshal(
		data,
		&state,
	); err != nil {
		return err
	}

	n.CurrentTerm = state.CurrentTerm
	n.VotedFor = state.VotedFor

	if len(state.Log) > 0 {
		n.Log = cloneLog(state.Log)
	}

	// CommitIndex was added after the original persistence
	// implementation. Older state files that do not contain
	// this field will simply restore it as zero.
	n.CommitIndex = state.CommitIndex

	// A corrupted or outdated state file must never cause
	// CommitIndex to point beyond the available log.
	lastLogIndex := uint64(0)

	if len(n.Log) > 0 {
		lastLogIndex = uint64(len(n.Log) - 1)
	}

	if n.CommitIndex > lastLogIndex {
		n.CommitIndex = lastLogIndex
	}

	return nil
}

// cloneLog creates a deep copy of a Raft log.
//
// This prevents the persistence layer from sharing command
// byte slices with the live Raft log.
func cloneLog(
	source []LogEntry,
) []LogEntry {

	if len(source) == 0 {
		return []LogEntry{
			{
				Term:    0,
				Command: nil,
			},
		}
	}

	result := make(
		[]LogEntry,
		len(source),
	)

	for i, entry := range source {

		result[i] = LogEntry{
			Term: entry.Term,
			Command: append(
				[]byte(nil),
				entry.Command...,
			),
		}
	}

	return result
}
