package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"raftstore/api"
	"raftstore/ml/controller"
	pb "raftstore/proto"
	"raftstore/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	nodeIDFlag := flag.String("id", "", "Node ID (node1, node2, node3)")
	batchSizeFlag := flag.Int("batchsize", 100, "Max batch size for Raft replication")
	pipelineDepthFlag := flag.Int("pipelinedepth", 1, "Max pipeline depth for replication")

	flag.Parse()

	nodeID := *nodeIDFlag
	if nodeID == "" && len(os.Args) >= 3 && os.Args[1] == "-id" {
		nodeID = os.Args[2]
	}

	if nodeID == "" {
		log.Fatal("Usage: go run ./cmd/server -id node1|node2|node3 [-batchsize 100] [-pipelinedepth 1]")
	}

	peers := map[string]string{
		"node1": "localhost:5001",
		"node2": "localhost:5002",
		"node3": "localhost:5003",
	}

	address, ok := peers[nodeID]
	if !ok {
		log.Fatalf("Unknown node ID %q. Use node1, node2, or node3.", nodeID)
	}

	nodePeers := make(map[string]string)
	for id, addr := range peers {
		if id != nodeID {
			nodePeers[id] = addr
		}
	}

	repConfig := raft.ReplicationConfig{
		MaxBatchSize:     *batchSizeFlag,
		MaxPipelineDepth: *pipelineDepthFlag,
	}

	node := raft.NewNode(nodeID, nodePeers, repConfig)

	log.Printf("Node %s initialized. MaxBatchSize: %d, MaxPipelineDepth: %d", nodeID, repConfig.MaxBatchSize, repConfig.MaxPipelineDepth)

	// gRPC SERVER
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Node %s failed to listen: %v", nodeID, err)
	}

	grpcServer := grpc.NewServer()
	raftServer := api.NewRaftServer(node)
	pb.RegisterRaftServiceServer(grpcServer, raftServer)

	log.Printf("Node %s running on gRPC %s", nodeID, address)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Node %s gRPC server stopped: %v", nodeID, err)
		}
	}()

	// HTTP METRICS & DASHBOARD SERVER
	metricsAddress := "localhost:" + metricsPort(nodeID)
	
	// Create a standard HTTP multiplexer to handle multiple routes
	mux := http.NewServeMux()
	
	// Mount existing Prometheus metrics
	metricsHandler := api.MetricsHandlerFromServer(raftServer)
	mux.Handle("/metrics", metricsHandler)

	// Mount new React Dashboard API endpoints
	dashAPI := api.NewDashboardAPI(node)
	mux.HandleFunc("/api/status", dashAPI.StatusHandler)
	mux.HandleFunc("/api/fault", dashAPI.FaultHandler)
	
	httpServer := &http.Server{
		Addr:    metricsAddress,
		Handler: mux,
	}

	log.Printf("Node %s HTTP API running on http://%s", nodeID, metricsAddress)

	go func() {
		err := httpServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Printf("Node %s HTTP server stopped: %v", nodeID, err)
		}
	}()

	log.Printf("Node %s metrics server running on http://%s/metrics", nodeID, metricsAddress)

	go func() {
		err := httpServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Printf("Node %s metrics server stopped: %v", nodeID, err)
		}
	}()

	// PEER CONNECTIONS
	for peerID, peerAddress := range nodePeers {
		go connectToPeer(node, peerID, peerAddress)
	}

	// ELECTION TIMER
	go node.StartElectionTimer()

	// XGBOOST ADAPTIVE CONTROLLER
	mlController := controller.NewMLController(raftServer, node)
	mlController.Start(10 * time.Second)

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

func connectToPeer(node *raft.Node, peerID string, address string) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		conn, err := grpc.DialContext(
			ctx,
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()

		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		client := pb.NewRaftServiceClient(conn)
		node.Mu.Lock()
		node.Clients[peerID] = client
		node.Mu.Unlock()
		return
	}
}