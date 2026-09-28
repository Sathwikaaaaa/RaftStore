package raft

import (
	"context"
	"fmt"

	pb "raftstore/proto"
)

func (n *Node) RequestVote(
	ctx context.Context,
	req *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {

	n.Mu.Lock()
	defer n.Mu.Unlock()

	// Reject stale terms.
	if req.Term < n.CurrentTerm {

		return &pb.RequestVoteResponse{
			Term:        n.CurrentTerm,
			VoteGranted: false,
		}, nil
	}

	// A newer term becomes the node's current term.
	if req.Term > n.CurrentTerm {

		n.CurrentTerm = req.Term

		n.State = Follower

		n.VotedFor = ""

		n.LeaderID = ""

		// Persist the new term before responding.
		if err := n.savePersistentState(); err != nil {
			return nil, err
		}
	}

	lastLogIndex := uint64(
		len(n.Log) - 1,
	)

	var lastLogTerm uint64

	if lastLogIndex > 0 {
		lastLogTerm =
			n.Log[lastLogIndex].Term
	}

	// Candidate log must be at least as up-to-date
	// as the follower's log.
	logIsUpToDate := false

	if req.LastLogTerm > lastLogTerm {

		logIsUpToDate = true

	} else if req.LastLogTerm == lastLogTerm &&
		req.LastLogIndex >= lastLogIndex {

		logIsUpToDate = true
	}

	canVote :=
		n.VotedFor == "" ||
			n.VotedFor == req.CandidateId

	if canVote && logIsUpToDate {

		n.VotedFor = req.CandidateId

		n.State = Follower

		n.LeaderID = ""

		// Persist the vote before granting it.
		if err := n.savePersistentState(); err != nil {
			return nil, err
		}

		// Reset election timer only when granting
		// the vote.
		select {

		case n.ElectionReset <- true:

		default:
		}

		return &pb.RequestVoteResponse{
			Term:        n.CurrentTerm,
			VoteGranted: true,
		}, nil
	}

	return &pb.RequestVoteResponse{
		Term:        n.CurrentTerm,
		VoteGranted: false,
	}, nil
}

func (n *Node) AppendEntries(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {

	n.Mu.Lock()
	defer n.Mu.Unlock()

	// Reject stale leaders.
	if req.Term < n.CurrentTerm {

		return &pb.AppendEntriesResponse{
			Term:          n.CurrentTerm,
			Success:       false,
			ConflictIndex: uint64(len(n.Log)),
		}, nil
	}

	stateChanged := false

	// A newer term becomes our current term.
	if req.Term > n.CurrentTerm {

		n.CurrentTerm = req.Term

		n.VotedFor = ""

		stateChanged = true
	}

	// Receiving valid AppendEntries means that the
	// sender is the current leader.
	n.State = Follower

	n.LeaderID = req.LeaderId

	// Reset election timer immediately.
	select {

	case n.ElectionReset <- true:

	default:
	}

	// If the term changed, persist it before continuing.
	if stateChanged {

		if err := n.savePersistentState(); err != nil {
			return nil, err
		}
	}

	// Check previous log index.
	if req.PrevLogIndex >= uint64(len(n.Log)) {

		return &pb.AppendEntriesResponse{
			Term:          n.CurrentTerm,
			Success:       false,
			ConflictIndex: uint64(len(n.Log)),
		}, nil
	}

	// Check previous log term.
	if n.Log[req.PrevLogIndex].Term !=
		req.PrevLogTerm {

		conflictTerm :=
			n.Log[req.PrevLogIndex].Term

		conflictIndex :=
			req.PrevLogIndex

		for conflictIndex > 0 &&
			n.Log[conflictIndex-1].Term ==
				conflictTerm {

			conflictIndex--
		}

		return &pb.AppendEntriesResponse{
			Term:          n.CurrentTerm,
			Success:       false,
			ConflictIndex: conflictIndex,
		}, nil
	}

	logChanged := false

	// Append incoming entries.
	for i, entry := range req.Entries {

		index :=
			req.PrevLogIndex +
				1 +
				uint64(i)

		if index < uint64(len(n.Log)) {

			// Existing entry conflicts with leader.
			if n.Log[index].Term != entry.Term {

				// A follower must never discard a committed
				// entry. This condition should not normally
				// occur in a correct Raft cluster.
				if index <= n.CommitIndex {
					return &pb.AppendEntriesResponse{
						Term:          n.CurrentTerm,
						Success:       false,
						ConflictIndex: index,
					}, nil
				}

				n.Log = n.Log[:index]

				n.Log = append(
					n.Log,
					LogEntry{
						Term: entry.Term,
						Command: append(
							[]byte(nil),
							entry.Command...,
						),
					},
				)

				logChanged = true
			}

		} else {

			n.Log = append(
				n.Log,
				LogEntry{
					Term: entry.Term,
					Command: append(
						[]byte(nil),
						entry.Command...,
					),
				},
			)

			logChanged = true
		}
	}

	// Persist any log changes before reporting
	// successful replication.
	if logChanged {

		if err := n.savePersistentState(); err != nil {
			return nil, err
		}
	}

	// Update commit index from the leader.
	if req.LeaderCommit > n.CommitIndex {

		lastLogIndex :=
			uint64(len(n.Log) - 1)

		newCommitIndex := req.LeaderCommit

		if newCommitIndex > lastLogIndex {
			newCommitIndex = lastLogIndex
		}

		if newCommitIndex > n.CommitIndex {

			n.CommitIndex = newCommitIndex

			// Persist the newly known commit index before
			// applying it to the state machine.
			if err := n.savePersistentState(); err != nil {
				return nil, err
			}

			n.applyCommittedEntriesLocked()
		}
	}

	return &pb.AppendEntriesResponse{
		Term:    n.CurrentTerm,
		Success: true,
	}, nil
}

// applyCommittedEntriesLocked applies all committed but
// not-yet-applied entries to the state machine.
//
// The caller must hold n.Mu.
func (n *Node) applyCommittedEntriesLocked() {

	for n.LastApplied < n.CommitIndex {

		n.LastApplied++

		if n.LastApplied >=
			uint64(len(n.Log)) {

			break
		}

		entry :=
			n.Log[n.LastApplied]

		command, err :=
			decodeCommand(entry.Command)

		if err != nil {
			continue
		}

		switch command.Operation {

		case "PUT":

			if err := n.KV.Put(
				command.Key,
				command.Value,
			); err != nil {
				fmt.Printf(
					"Node %s failed to apply PUT at log index %d: %v\n",
					n.ID,
					n.LastApplied,
					err,
				)
				continue
			}

			fmt.Printf(
				"Node %s applied log index %d : PUT %s\n",
				n.ID,
				n.LastApplied,
				command.Key,
			)

		case "DELETE":

			if err := n.KV.Delete(
				command.Key,
			); err != nil {
				fmt.Printf(
					"Node %s failed to apply DELETE at log index %d: %v\n",
					n.ID,
					n.LastApplied,
					err,
				)
				continue
			}

			fmt.Printf(
				"Node %s applied log index %d : DELETE %s\n",
				n.ID,
				n.LastApplied,
				command.Key,
			)
		}
	}
}
