package raft

import (
	"context"
	"time"

	pb "raftstore/proto"
)

// Put handles a client PUT request.
//
// The command is first appended to the Raft log.
// It becomes visible in the state machine only after
// the entry has been committed by a majority.
func (n *Node) Put(
	ctx context.Context,
	req *pb.PutRequest,
) (*pb.PutResponse, error) {

	n.Mu.Lock()

	if n.State != Leader {

		leaderID := n.findLeaderIDLocked()

		n.Mu.Unlock()

		return &pb.PutResponse{
			Success:  false,
			LeaderId: leaderID,
			Message:  "node is not leader",
		}, nil
	}

	command, err := encodeCommand(
		"PUT",
		req.Key,
		req.Value,
	)

	if err != nil {
		n.Mu.Unlock()

		return &pb.PutResponse{
			Success:  false,
			LeaderId: n.ID,
			Message:  "failed to encode command",
		}, nil
	}

	entry := LogEntry{
		Term:    n.CurrentTerm,
		Command: command,
	}

	// Append the new command to the local Raft log.
	n.Log = append(
		n.Log,
		entry,
	)

	index := uint64(len(n.Log) - 1)

	// Persist the log before replication.
	if err := n.savePersistentState(); err != nil {

		// Roll back the in-memory append because the
		// persistent state was not successfully stored.
		n.Log = n.Log[:len(n.Log)-1]

		n.Mu.Unlock()

		return &pb.PutResponse{
			Success:  false,
			LeaderId: n.ID,
			Message:  "failed to persist Raft log",
		}, nil
	}

	n.Mu.Unlock()

	// Replicate the newly appended entry.
	n.replicateToAllPeers()

	// Wait until the entry becomes committed.
	if !n.waitForCommit(
		index,
		5*time.Second,
	) {

		n.Mu.Lock()

		leaderID := n.findLeaderIDLocked()

		n.Mu.Unlock()

		return &pb.PutResponse{
			Success:  false,
			LeaderId: leaderID,
			Message:  "PUT timed out waiting for majority commit",
		}, nil
	}

	return &pb.PutResponse{
		Success:  true,
		LeaderId: n.ID,
		Message: "PUT committed at index " +
			uintToString(index),
	}, nil
}

// Get handles a client GET request.
//
// Reads are served from the committed state machine.
func (n *Node) Get(
	ctx context.Context,
	req *pb.GetRequest,
) (*pb.GetResponse, error) {

	n.Mu.Lock()

	if n.State != Leader {

		leaderID := n.findLeaderIDLocked()

		n.Mu.Unlock()

		return &pb.GetResponse{
			Found:    false,
			Value:    "",
			LeaderId: leaderID,
			Message:  "node is not leader",
		}, nil
	}

	value, ok, err := n.KV.Get(req.Key)

	n.Mu.Unlock()

	if err != nil {
		return &pb.GetResponse{
			Found:    false,
			Value:    "",
			LeaderId: n.ID,
			Message:  "GET failed: " + err.Error(),
		}, nil
	}

	if !ok {
		return &pb.GetResponse{
			Found:    false,
			Value:    "",
			LeaderId: n.ID,
			Message:  "GET successful",
		}, nil
	}
	return &pb.GetResponse{
		Found:    true,
		Value:    value,
		LeaderId: n.ID,
		Message:  "GET successful",
	}, nil
}

// Delete handles a client DELETE request.
func (n *Node) Delete(
	ctx context.Context,
	req *pb.DeleteRequest,
) (*pb.DeleteResponse, error) {

	n.Mu.Lock()

	if n.State != Leader {

		leaderID := n.findLeaderIDLocked()

		n.Mu.Unlock()

		return &pb.DeleteResponse{
			Success:  false,
			LeaderId: leaderID,
			Message:  "node is not leader",
		}, nil
	}

	command, err := encodeCommand(
		"DELETE",
		req.Key,
		"",
	)

	if err != nil {
		n.Mu.Unlock()

		return &pb.DeleteResponse{
			Success:  false,
			LeaderId: n.ID,
			Message:  "failed to encode command",
		}, nil
	}

	entry := LogEntry{
		Term:    n.CurrentTerm,
		Command: command,
	}

	n.Log = append(
		n.Log,
		entry,
	)

	index := uint64(len(n.Log) - 1)

	// Persist the new log entry.
	if err := n.savePersistentState(); err != nil {

		// Roll back if persistence fails.
		n.Log = n.Log[:len(n.Log)-1]

		n.Mu.Unlock()

		return &pb.DeleteResponse{
			Success:  false,
			LeaderId: n.ID,
			Message:  "failed to persist Raft log",
		}, nil
	}

	n.Mu.Unlock()

	// Replicate the delete operation.
	n.replicateToAllPeers()

	// Wait for majority commit.
	if !n.waitForCommit(
		index,
		5*time.Second,
	) {

		n.Mu.Lock()

		leaderID := n.findLeaderIDLocked()

		n.Mu.Unlock()

		return &pb.DeleteResponse{
			Success:  false,
			LeaderId: leaderID,
			Message:  "DELETE timed out waiting for majority commit",
		}, nil
	}

	return &pb.DeleteResponse{
		Success:  true,
		LeaderId: n.ID,
		Message: "DELETE committed at index " +
			uintToString(index),
	}, nil
}

// waitForCommit waits until a particular Raft log entry
// has been committed.
func (n *Node) waitForCommit(
	index uint64,
	timeout time.Duration,
) bool {

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {

		n.Mu.Lock()

		committed := n.CommitIndex >= index

		n.Mu.Unlock()

		if committed {
			return true
		}

		time.Sleep(10 * time.Millisecond)
	}

	return false
}

// findLeaderIDLocked returns the best known leader.
//
// The caller must hold n.Mu.
func (n *Node) findLeaderIDLocked() string {

	if n.State == Leader {
		return n.ID
	}

	return n.LeaderID
}

// uintToString converts a uint64 to a string without
// pulling extra dependencies into the project.
func uintToString(value uint64) string {

	if value == 0 {
		return "0"
	}

	var digits [20]byte

	i := len(digits)

	for value > 0 {

		i--

		digits[i] = byte(
			'0' + value%10,
		)

		value /= 10
	}

	return string(digits[i:])
}
