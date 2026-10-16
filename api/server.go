package api

import (
	"context"
	"time"

	"raftstore/metrics"
	pb "raftstore/proto"
	"raftstore/raft"
)

type Server struct {
	pb.UnimplementedRaftServiceServer

	Node *raft.Node

	Latency    *metrics.LatencyRecorder
	Throughput *metrics.ThroughputRecorder

	RaftTracker *metrics.RaftMetricsTracker

	SnapshotCollector *metrics.SnapshotCollector
}

func NewRaftServer(node *raft.Node) *Server {

	latency :=
		metrics.NewLatencyRecorder()

	throughput :=
		metrics.NewThroughputRecorder()

	throughput.Start()

	raftTracker :=
		metrics.NewRaftMetricsTracker()

	snapshotCollector :=
		metrics.NewSnapshotCollector(
			latency,
			throughput,
			raftTracker,
		)

	return &Server{
		Node: node,

		Latency: latency,

		Throughput: throughput,

		RaftTracker: raftTracker,

		SnapshotCollector: snapshotCollector,
	}
}

func (s *Server) RequestVote(
	ctx context.Context,
	req *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {

	return s.Node.RequestVote(
		ctx,
		req,
	)
}

func (s *Server) AppendEntries(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {

	return s.Node.AppendEntries(
		ctx,
		req,
	)
}

func (s *Server) Put(
	ctx context.Context,
	req *pb.PutRequest,
) (*pb.PutResponse, error) {

	start := time.Now()

	response, err :=
		s.Node.Put(
			ctx,
			req,
		)

	s.Latency.Record(
		time.Since(start),
	)

	s.Throughput.Record()

	return response, err
}

func (s *Server) Get(
	ctx context.Context,
	req *pb.GetRequest,
) (*pb.GetResponse, error) {

	start := time.Now()

	response, err :=
		s.Node.Get(
			ctx,
			req,
		)

	s.Latency.Record(
		time.Since(start),
	)

	s.Throughput.Record()

	return response, err
}

func (s *Server) Delete(
	ctx context.Context,
	req *pb.DeleteRequest,
) (*pb.DeleteResponse, error) {

	start := time.Now()

	response, err :=
		s.Node.Delete(
			ctx,
			req,
		)

	s.Latency.Record(
		time.Since(start),
	)

	s.Throughput.Record()

	return response, err
}

// LatencyStats returns the current request latency statistics.
func (s *Server) LatencyStats() metrics.LatencyStats {

	return s.Latency.Stats()
}

// Operations returns the total number of client operations
// observed by this API server.
func (s *Server) Operations() uint64 {

	return s.Throughput.Operations()
}

// OperationsPerSecond returns the current throughput rate.
func (s *Server) OperationsPerSecond() float64 {

	return s.Throughput.OperationsPerSecond()
}

// MetricsSnapshot returns a complete point-in-time
// monitoring snapshot for this RaftStore node.
func (s *Server) MetricsSnapshot() metrics.Snapshot {

	return s.SnapshotCollector.Collect(
		s.Node,
	)
}
