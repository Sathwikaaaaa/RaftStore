package raft

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	pb "raftstore/proto"
)

const (
	minElectionTimeout = 3000 * time.Millisecond
	maxElectionTimeout = 6000 * time.Millisecond

	heartbeatInterval = 150 * time.Millisecond
)

func randomElectionTimeout() time.Duration {

	return minElectionTimeout +
		time.Duration(
			rand.Int63n(
				int64(
					maxElectionTimeout-
						minElectionTimeout,
				),
			),
		)
}

// StartElectionTimer continuously waits for either:
//
// 1. A heartbeat/vote that resets the election timer.
// 2. The election timeout expiring.
//
// When the timeout expires, the node starts an election.
func (n *Node) StartElectionTimer() {

	for {

		timeout :=
			randomElectionTimeout()

		timer :=
			time.NewTimer(timeout)

		select {

		case <-timer.C:

			n.Mu.Lock()

			if n.State == Leader {

				n.Mu.Unlock()

				continue
			}

			n.Mu.Unlock()

			n.startElection()

		case <-n.ElectionReset:

			if !timer.Stop() {

				select {

				case <-timer.C:

				default:
				}
			}

			continue

		case <-n.StopChan:

			if !timer.Stop() {

				select {

				case <-timer.C:

				default:
				}
			}

			return
		}
	}
}

func (n *Node) startElection() {

	n.Mu.Lock()

	// Become candidate.
	n.State = Candidate

	n.CurrentTerm++

	term := n.CurrentTerm

	n.VotedFor = n.ID

	n.LeaderID = ""

	n.VotesReceived = 1

	lastLogIndex :=
		uint64(len(n.Log) - 1)

	var lastLogTerm uint64

	if lastLogIndex > 0 {

		lastLogTerm =
			n.Log[lastLogIndex].Term
	}

	// Persist the new term and vote before
	// requesting votes.
	if err := n.savePersistentState(); err != nil {

		n.State = Follower

		n.VotedFor = ""

		n.Mu.Unlock()

		fmt.Printf(
			"Node %s failed to persist election state: %v\n",
			n.ID,
			err,
		)

		return
	}

	fmt.Printf(
		"Node %s became CANDIDATE for term %d\n",
		n.ID,
		term,
	)

	// Reset election timer while conducting
	// the election.
	select {

	case n.ElectionReset <- true:

	default:
	}

	n.Mu.Unlock()

	// Request votes from all peers.
	for peerID, client := range n.Clients {

		go func(
			peerID string,
			client pb.RaftServiceClient,
		) {

			ctx, cancel :=
				context.WithTimeout(
					context.Background(),
					500*time.Millisecond,
				)

			defer cancel()

			response, err :=
				client.RequestVote(
					ctx,
					&pb.RequestVoteRequest{
						Term:         term,
						CandidateId:  n.ID,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
					},
				)

			if err != nil {
				return
			}

			n.Mu.Lock()
			defer n.Mu.Unlock()

			// Ignore responses from old elections.
			if term != n.CurrentTerm {
				return
			}

			// A newer term always wins.
			if response.Term >
				n.CurrentTerm {

				n.CurrentTerm =
					response.Term

				n.State = Follower

				n.VotedFor = ""

				n.LeaderID = ""

				if err := n.savePersistentState(); err != nil {

					fmt.Printf(
						"Node %s failed to persist newer term: %v\n",
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

			if n.State != Candidate {
				return
			}

			if response.VoteGranted {

				n.VotesReceived++

				fmt.Printf(
					"Node %s received vote from %s (%d votes)\n",
					n.ID,
					peerID,
					n.VotesReceived,
				)

				majority :=
					(len(n.Peers)+1)/2 + 1

				if n.VotesReceived >=
					majority {

					n.becomeLeaderLocked()
				}
			}

		}(peerID, client)
	}
}

func (n *Node) becomeLeaderLocked() {

	if n.State == Leader {
		return
	}

	n.State = Leader

	n.LeaderID = n.ID

	nextIndex :=
		uint64(len(n.Log))

	for peerID := range n.Peers {

		n.NextIndex[peerID] =
			nextIndex

		n.MatchIndex[peerID] = 0
	}

	fmt.Printf(
		"🔥 Node %s became LEADER for term %d\n",
		n.ID,
		n.CurrentTerm,
	)

	// Send an immediate heartbeat.
	go n.sendHeartbeats()

	// Reset election timer.
	select {

	case n.ElectionReset <- true:

	default:
	}
}

func (n *Node) sendHeartbeats() {

	ticker :=
		time.NewTicker(
			heartbeatInterval,
		)

	defer ticker.Stop()

	for {

		n.Mu.Lock()

		if n.State != Leader {

			n.Mu.Unlock()

			return
		}

		peers :=
			make(
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

		select {

		case <-ticker.C:

			continue

		case <-n.StopChan:

			return
		}
	}
}
