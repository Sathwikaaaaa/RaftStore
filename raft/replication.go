package raft

import (
	"context"
	"log"
	"time"

	pb "raftstore/proto"
)

// replicateToPeer sends the appropriate log entries to one follower.
func (n *Node) replicateToPeer(peerID string) {

	n.Mu.Lock()

	if n.State != Leader {
		n.Mu.Unlock()
		return
	}

	client, ok := n.Clients[peerID]

	if !ok {
		n.Mu.Unlock()
		return
	}

	nextIndex := n.NextIndex[peerID]

	if nextIndex == 0 {
		nextIndex = 1
	}

	if nextIndex > uint64(len(n.Log)) {
		nextIndex = uint64(len(n.Log))
	}

	prevLogIndex := nextIndex - 1

	prevLogTerm :=
		n.Log[prevLogIndex].Term

	entries := make(
		[]*pb.LogEntry,
		0,
	)

	for i := nextIndex; i < uint64(len(n.Log)); i++ {

		entry := n.Log[i]

		entries = append(
			entries,
			&pb.LogEntry{
				Term: entry.Term,
				Command: append(
					[]byte(nil),
					entry.Command...,
				),
			},
		)
	}

	term := n.CurrentTerm

	leaderCommit := n.CommitIndex

	leaderID := n.ID

	n.Mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		500*time.Millisecond,
	)

	defer cancel()

	resp, err := client.AppendEntries(
		ctx,
		&pb.AppendEntriesRequest{
			Term:         term,
			LeaderId:     leaderID,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		},
	)

	if err != nil {
		return
	}

	n.Mu.Lock()
	defer n.Mu.Unlock()

	// Ignore responses from an old leadership term.
	if n.State != Leader ||
		n.CurrentTerm != term {

		return
	}

	// Follower has a newer term.
	if resp.Term > n.CurrentTerm {

		n.CurrentTerm = resp.Term

		n.State = Follower

		n.VotedFor = ""

		n.LeaderID = ""

		// Persist the newer term.
		if err := n.savePersistentState(); err != nil {
			log.Printf(
				"Node %s failed to persist newer term: %v",
				n.ID,
				err,
			)
		}

		select {

		case n.ElectionReset <- true:

		default:
		}

		return
	}

	if resp.Success {

		matchIndex :=
			prevLogIndex +
				uint64(len(entries))

		n.MatchIndex[peerID] =
			matchIndex

		n.NextIndex[peerID] =
			matchIndex + 1

		n.advanceCommitIndexLocked()

		return
	}

	// Replication failed because the follower's
	// log does not match.
	if n.NextIndex[peerID] > 1 {

		if resp.ConflictIndex > 0 {

			n.NextIndex[peerID] =
				resp.ConflictIndex

		} else {

			n.NextIndex[peerID]--
		}
	}

	log.Printf(
		"Node %s adjusting nextIndex for %s to %d",
		n.ID,
		peerID,
		n.NextIndex[peerID],
	)
}

// replicateToAllPeers sends the current leader's log
// to all followers.
func (n *Node) replicateToAllPeers() {

	n.Mu.Lock()

	if n.State != Leader {
		n.Mu.Unlock()
		return
	}

	peers := make(
		[]string,
		0,
		len(n.Peers),
	)

	for peerID := range n.Peers {
		peers = append(
			peers,
			peerID,
		)
	}

	n.Mu.Unlock()

	for _, peerID := range peers {

		go n.replicateToPeer(
			peerID,
		)
	}
}

// advanceCommitIndexLocked advances the leader commit
// index when a majority has replicated an entry.
//
// The caller must hold n.Mu.
func (n *Node) advanceCommitIndexLocked() {

	if n.State != Leader {
		return
	}

	lastIndex :=
		uint64(len(n.Log) - 1)

	for index :=
		n.CommitIndex + 1; index <= lastIndex; index++ {

		// A leader commits an entry directly only when
		// the entry belongs to the leader's current term.
		if n.Log[index].Term !=
			n.CurrentTerm {

			continue
		}

		replicated := 1

		// Count replicated followers.
		for peerID := range n.Peers {

			if n.MatchIndex[peerID] >=
				index {

				replicated++
			}
		}

		clusterSize :=
			len(n.Peers) + 1

		majority :=
			clusterSize/2 + 1

		if replicated >= majority {

			n.CommitIndex = index

			// Persist the new commit index before applying
			// the command to the state machine.
			if err := n.savePersistentState(); err != nil {

				log.Printf(
					"Node %s failed to persist commit index %d: %v",
					n.ID,
					index,
					err,
				)
			}

			n.applyCommittedEntriesLocked()

			log.Printf(
				"Node %s committed log index %d (%d/%d)",
				n.ID,
				index,
				replicated,
				clusterSize,
			)
		}
	}
}
