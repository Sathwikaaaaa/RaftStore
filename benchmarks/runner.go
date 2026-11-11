package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "raftstore/proto"
)

type BenchmarkRunner struct {
	Address     string
	Concurrency int
	Requests    []OperationRequest
}

func NewBenchmarkRunner(address string, concurrency int, requests []OperationRequest) *BenchmarkRunner {
	return &BenchmarkRunner{
		Address:     address,
		Concurrency: concurrency,
		Requests:    requests,
	}
}

func (r *BenchmarkRunner) Run() (LatencyStats, error) {
	conn, err := grpc.Dial(r.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return LatencyStats{}, fmt.Errorf("failed to connect to gRPC server at %s: %v", r.Address, err)
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)

	reqChan := make(chan OperationRequest, len(r.Requests))
	for _, req := range r.Requests {
		reqChan <- req
	}
	close(reqChan)

	var mu sync.Mutex
	var latencies []time.Duration
	var failedCount int

	startTime := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < r.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			// Worker local connection or shared client (gRPC client is safe for concurrent use)
			for req := range reqChan {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				opStart := time.Now()

				var callErr error
				switch req.Op {
				case "put":
					_, callErr = client.Put(ctx, &pb.PutRequest{Key: req.Key, Value: req.Value})
				case "get":
					_, callErr = client.Get(ctx, &pb.GetRequest{Key: req.Key})
				case "delete":
					_, callErr = client.Delete(ctx, &pb.DeleteRequest{Key: req.Key})
				}

				duration := time.Since(opStart)
				cancel()

				mu.Lock()
				if callErr != nil {
					failedCount++
				} else {
					latencies = append(latencies, duration)
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	stats := CalculateStats(latencies, failedCount, totalDuration)
	return stats, nil
}