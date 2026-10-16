package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"raftstore/api"
	pb "raftstore/proto"
	"raftstore/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	if len(os.Args) < 3 || os.Args[1] != "-id" {
		log.Fatal(
			"Usage: go run .\\cmd\\server -id node1|node2|node3",
		)
	}

	nodeID := os.Args[2]

	// Fixed cluster configuration.
	peers := map[string]string{
		"node1": "localhost:5001",
		"node2": "localhost:5002",
		"node3": "localhost:5003",
	}

	// Make sure the supplied node exists.
	address, ok := peers[nodeID]

	if !ok {
		log.Fatalf(
			"Unknown node ID %q. Use node1, node2, or node3.",
			nodeID,
		)
	}

	// Remove the current node from its peer list.
	nodePeers := make(map[string]string)

	for id, addr := range peers {

		if id != nodeID {
			nodePeers[id] = addr
		}
	}

	// Create the Raft node.
	node := raft.NewNode(
		nodeID,
		nodePeers,
	)

	// ------------------------------------------------------------
	// gRPC SERVER
	// ------------------------------------------------------------

	listener, err := net.Listen(
		"tcp",
		address,
	)

	if err != nil {
		log.Fatalf(
			"Node %s failed to listen: %v",
			nodeID,
			err,
		)
	}

	grpcServer := grpc.NewServer()

	// Create ONE API server instance.

	// This is important because the API server owns the
	// latency, throughput, Raft metrics, and snapshot collectors.
	raftServer := api.NewRaftServer(node)

	pb.RegisterRaftServiceServer(
		grpcServer,
		raftServer,
	)

	log.Printf(
		"Node %s running on gRPC %s",
		nodeID,
		address,
	)

	// Start accepting gRPC requests.
	go func() {

		if err := grpcServer.Serve(listener); err != nil {

			log.Fatalf(
				"Node %s gRPC server stopped: %v",
				nodeID,
				err,
			)
		}

	}()

	// ------------------------------------------------------------
	// HTTP METRICS SERVER
	// ------------------------------------------------------------

	metricsAddress :=
		"localhost:" + metricsPort(nodeID)

	metricsHandler :=
		api.MetricsHandlerFromServer(
			raftServer,
		)

	httpServer := &http.Server{
		Addr:    metricsAddress,
		Handler: metricsHandler,
	}

	log.Printf(
		"Node %s metrics server running on http://%s/metrics",
		nodeID,
		metricsAddress,
	)

	go func() {

		err := httpServer.ListenAndServe()

		if err != nil &&
			err != http.ErrServerClosed {

			log.Printf(
				"Node %s metrics server stopped: %v",
				nodeID,
				err,
			)
		}

	}()

	// ------------------------------------------------------------
	// PEER CONNECTIONS
	// ------------------------------------------------------------

	for peerID, peerAddress := range nodePeers {

		go connectToPeer(
			node,
			peerID,
			peerAddress,
		)
	}

	// ------------------------------------------------------------
	// ELECTION TIMER
	// ------------------------------------------------------------

	go node.StartElectionTimer()

	// ------------------------------------------------------------
	// KEEP SERVER ALIVE
	// ------------------------------------------------------------

	select {}
}

func metricsPort(nodeID string) string {

	switch nodeID {

	case "node1":
		return "6001"

	case "node2":
		return "6002"

	case "node3":
		return "6003"

	default:
		return "6099"
	}
}

func connectToPeer(
	node *raft.Node,
	peerID string,
	address string,
) {

	for {

		ctx, cancel := context.WithTimeout(
			context.Background(),
			1*time.Second,
		)

		conn, err := grpc.DialContext(
			ctx,
			address,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
			grpc.WithBlock(),
		)

		cancel()

		if err != nil {

			log.Printf(
				"Node %s could not connect to %s at %s: %v",
				node.ID,
				peerID,
				address,
				err,
			)

			time.Sleep(
				1 * time.Second,
			)

			continue
		}

		client := pb.NewRaftServiceClient(conn)

		node.Mu.Lock()

		node.Clients[peerID] = client

		node.Mu.Unlock()

		log.Printf(
			"Node %s connected to %s",
			node.ID,
			peerID,
		)

		return
	}
}
