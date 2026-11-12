package tests

import (
	"context"
	"log"
	"math/rand"
	"net"
	"testing"
	"time"

	"raftstore/api"
	pb "raftstore/proto"
	"raftstore/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func connectTestPeers(node *raft.Node, nodePeers map[string]string) {
	for peerID, address := range nodePeers {
		go func(pID, addr string) {
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				conn, err := grpc.DialContext(
					ctx,
					addr,
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithBlock(),
				)
				cancel()

				if err != nil {
					time.Sleep(200 * time.Millisecond)
					continue
				}

				client := pb.NewRaftServiceClient(conn)
				node.Mu.Lock()
				if node.Clients == nil {
					node.Clients = make(map[string]pb.RaftServiceClient)
				}
				node.Clients[pID] = client
				node.Mu.Unlock()
				return
			}
		}(peerID, address)
	}
}

func spawnTestNode(id string, port string, peers map[string]string) (*raft.Node, *grpc.Server, func()) {
	config := raft.DefaultReplicationConfig()
	node := raft.NewNode(id, peers, config)
	node.ElectionReset = make(chan bool, 100)

	server := api.NewRaftServer(node)
	grpcServer := grpc.NewServer()
	pb.RegisterRaftServiceServer(grpcServer, server)

	addr := "localhost:" + port
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", addr, err)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("grpcServer.Serve error for %s: %v", id, err)
		}
	}()

	cleanup := func() {
		select {
		case node.StopChan <- true:
		default:
		}
		grpcServer.Stop()
	}

	return node, grpcServer, cleanup
}

func TestLeaderCrashAndRecovery(t *testing.T) {
	log.Println("=== Starting Fault Injection: Leader Crash & Recovery ===")

	peers1 := map[string]string{"node2": "localhost:5012", "node3": "localhost:5013"}
	peers2 := map[string]string{"node1": "localhost:5011", "node3": "localhost:5013"}
	peers3 := map[string]string{"node1": "localhost:5011", "node2": "localhost:5012"}

	n1, _, cleanup1 := spawnTestNode("node1", "5011", peers1)
	defer cleanup1()

	n2, _, cleanup2 := spawnTestNode("node2", "5012", peers2)
	defer cleanup2()

	n3, _, cleanup3 := spawnTestNode("node3", "5013", peers3)
	defer cleanup3()

	time.Sleep(2 * time.Second)

	connectTestPeers(n1, peers1)
	connectTestPeers(n2, peers2)
	connectTestPeers(n3, peers3)

	time.Sleep(2 * time.Second)

	go n1.StartElectionTimer()
	go n2.StartElectionTimer()
	go n3.StartElectionTimer()

	// Increased timeout to 8 seconds to guarantee election finality
	time.Sleep(8 * time.Second)

	nodes := []*raft.Node{n1, n2, n3}
	var leader *raft.Node

	for _, n := range nodes {
		n.Mu.Lock()
		if n.State == raft.Leader {
			leader = n
		}
		n.Mu.Unlock()
	}

	if leader == nil {
		t.Fatalf("Cluster failed to elect a leader.")
	}

	log.Printf("Elected Leader: %s. Initiating crash...", leader.ID)

	leader.Mu.Lock()
	leader.State = raft.Follower
	leader.CurrentTerm = 0
	leader.Mu.Unlock()

	select {
	case leader.StopChan <- true:
	default:
	}

	startRecovery := time.Now()
	
	// Increased recovery wait time
	time.Sleep(8 * time.Second)

	var newLeader *raft.Node
	for _, n := range nodes {
		if n.ID == leader.ID {
			continue 
		}
		n.Mu.Lock()
		if n.State == raft.Leader {
			newLeader = n
		}
		n.Mu.Unlock()
	}

	if newLeader == nil {
		t.Fatalf("Cluster failed to recover and elect a new leader after crash")
	}

	recoveryTime := time.Since(startRecovery)
	log.Printf("Recovery successful! New Leader: %s. Recovery Time: %v", newLeader.ID, recoveryTime)

	if newLeader.ID == leader.ID {
		t.Fatalf("Crashed leader was incorrectly re-elected")
	}
}

func TestNetworkPartitionSplitBrain(t *testing.T) {
	log.Println("=== Starting Fault Injection: Network Partition (Split-Brain) ===")

	peers4 := map[string]string{"node5": "localhost:5025", "node6": "localhost:5026"}
	peers5 := map[string]string{"node4": "localhost:5024", "node6": "localhost:5026"}
	peers6 := map[string]string{"node4": "localhost:5024", "node5": "localhost:5025"}

	n4, _, cleanup4 := spawnTestNode("node4", "5024", peers4)
	defer cleanup4()

	n5, _, cleanup5 := spawnTestNode("node5", "5025", peers5)
	defer cleanup5()

	n6, _, cleanup6 := spawnTestNode("node6", "5026", peers6)
	defer cleanup6()

	time.Sleep(2 * time.Second)

	connectTestPeers(n4, peers4)
	connectTestPeers(n5, peers5)
	connectTestPeers(n6, peers6)

	time.Sleep(2 * time.Second)

	go n4.StartElectionTimer()
	go n5.StartElectionTimer()
	go n6.StartElectionTimer()

	// Increased timeout to 8 seconds
	time.Sleep(8 * time.Second)

	nodes := []*raft.Node{n4, n5, n6}
	var oldLeader *raft.Node

	for _, n := range nodes {
		n.Mu.Lock()
		if n.State == raft.Leader {
			oldLeader = n
		}
		n.Mu.Unlock()
	}

	if oldLeader == nil {
		t.Fatalf("Cluster failed to elect initial leader")
	}
	oldTerm := oldLeader.CurrentTerm
	log.Printf("Initial Leader: %s (Term %d). Initiating network partition...", oldLeader.ID, oldTerm)

	oldLeader.Mu.Lock()
	for peerID := range oldLeader.Clients {
		delete(oldLeader.Clients, peerID)
	}
	oldLeader.Mu.Unlock()

	for _, n := range nodes {
		if n.ID != oldLeader.ID {
			n.Mu.Lock()
			delete(n.Clients, oldLeader.ID)
			n.Mu.Unlock()
		}
	}

	// Increased partition wait time
	time.Sleep(8 * time.Second)

	var newLeader *raft.Node
	for _, n := range nodes {
		if n.ID == oldLeader.ID {
			continue 
		}
		n.Mu.Lock()
		if n.State == raft.Leader {
			newLeader = n
		}
		n.Mu.Unlock()
	}

	if newLeader == nil {
		t.Fatalf("Partitioned nodes failed to elect a new leader")
	}
	newTerm := newLeader.CurrentTerm
	log.Printf("Partition successful! New Leader: %s (Term %d)", newLeader.ID, newTerm)

	if newTerm <= oldTerm {
		t.Fatalf("New leader term (%d) must be greater than old leader term (%d)", newTerm, oldTerm)
	}

	log.Println("Healing network partition...")
	
	oldLeaderPeers := peers4
	if oldLeader.ID == "node5" {
		oldLeaderPeers = peers5
	} else if oldLeader.ID == "node6" {
		oldLeaderPeers = peers6
	}
	connectTestPeers(oldLeader, oldLeaderPeers)

	for _, n := range nodes {
		if n.ID != oldLeader.ID {
			nPeers := peers4
			if n.ID == "node5" {
				nPeers = peers5
			} else if n.ID == "node6" {
				nPeers = peers6
			}
			
			reconnectMap := map[string]string{oldLeader.ID: nPeers[oldLeader.ID]}
			connectTestPeers(n, reconnectMap)
		}
	}

	// Give old leader time to receive new leader's heartbeat
	time.Sleep(4 * time.Second) 
	
	oldLeader.Mu.Lock()
	stateAfterHeal := oldLeader.State
	termAfterHeal := oldLeader.CurrentTerm
	oldLeader.Mu.Unlock()

	log.Printf("Old Leader %s resolved to State: %v (Term %d)", oldLeader.ID, stateAfterHeal, termAfterHeal)

	if stateAfterHeal == raft.Leader {
		t.Fatalf("Split-brain failure: Old leader failed to step down after partition healed")
	}
	if termAfterHeal < newTerm {
		t.Fatalf("Old leader failed to update its term to the current cluster term")
	}
	
	log.Println("Split-brain resolved successfully. Cluster maintains a single leader.")
}