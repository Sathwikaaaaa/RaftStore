package raft

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	pb "raftstore/proto"
)

// Track active in-flight requests per peer for pipelining
var inFlightRequests = make(map[string]*int32)

// replicateToPeer sends log entries to one follower with pipeline support.
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
	prevLogTerm := n.Log[prevLogIndex].Term

	maxBatch := n.RepConfig.MaxBatchSize
	if maxBatch <= 0 {
		maxBatch = 100
	}

	maxPipeline := n.RepConfig.MaxPipelineDepth
	if maxPipeline <= 0 {
		maxPipeline = 1
	}

	// Initialize tracking for peer if missing
	if _, exists := inFlightRequests[peerID]; !exists {
		var val int32
		inFlightRequests[peerID] = &val
	}

	currentInFlight := atomic.LoadInt32(inFlightRequests[peerID])
	if int(currentInFlight) >= maxPipeline {
		n.Mu.Unlock()
		return // Pipeline depth limit reached for this peer
	}

	entries := make([]*pb.LogEntry, 0)
	for i := nextIndex; i < uint64(len(n.Log)); i++ {
		if len(entries) >= maxBatch {
			break
		}

		entry := n.Log[i]
		entries = append(entries, &pb.LogEntry{
			Term: entry.Term,
			Command: append([]byte(nil), entry.Command...),
		})
	}

	term := n.CurrentTerm
	leaderCommit := n.CommitIndex
	leaderID := n.ID

	n.Mu.Unlock()

	atomic.AddInt32(inFlightRequests[peerID], 1)
	defer atomic.AddInt32(inFlightRequests[peerID], -1)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resp, err := client.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	})

	if err != nil {
		return
	}

	n.Mu.Lock()
	defer n.Mu.Unlock()

	if n.State != Leader || n.CurrentTerm != term {
		return
	}

	if resp.Term > n.CurrentTerm {
		n.CurrentTerm = resp.Term
		n.State = Follower
		n.VotedFor = ""
		n.LeaderID = ""
		if err := n.savePersistentState(); err != nil {
			log.Printf("Node %s failed to persist newer term: %v", n.ID, err)
		}
		select {
		case n.ElectionReset <- true:
		default:
		}
		return
	}

	if resp.Success {
		matchIndex := prevLogIndex + uint64(len(entries))
		if matchIndex > n.MatchIndex[peerID] {
			n.MatchIndex[peerID] = matchIndex
			n.NextIndex[peerID] = matchIndex + 1
		}
		n.advanceCommitIndexLocked()
		return
	}

	if n.NextIndex[peerID] > 1 {
		if resp.ConflictIndex > 0 {
			n.NextIndex[peerID] = resp.ConflictIndex
		} else {
			n.NextIndex[peerID]--
		}
	}
}

func (n *Node) replicateToAllPeers() {
	n.Mu.Lock()
	if n.State != Leader {
		n.Mu.Unlock()
		return
	}

	peers := make([]string, 0, len(n.Peers))
	for peerID := range n.Peers {
		peers = append(peers, peerID)
	}
	n.Mu.Unlock()

	for _, peerID := range peers {
		go n.replicateToPeer(peerID)
	}
}

func (n *Node) advanceCommitIndexLocked() {
	if n.State != Leader {
		return
	}

	lastIndex := uint64(len(n.Log) - 1)

	for index := n.CommitIndex + 1; index <= lastIndex; index++ {
		if n.Log[index].Term != n.CurrentTerm {
			continue
		}

		replicated := 1
		for peerID := range n.Peers {
			if n.MatchIndex[peerID] >= index {
				replicated++
			}
		}

		clusterSize := len(n.Peers) + 1
		majority := clusterSize/2 + 1

		if replicated >= majority {
			n.CommitIndex = index
			if err := n.savePersistentState(); err != nil {
				log.Printf("Node %s failed to persist commit index %d: %v", n.ID, index, err)
			}
			n.applyCommittedEntriesLocked()
		}
	}
}