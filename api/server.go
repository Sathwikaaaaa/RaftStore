package api

import (
	"context"

	pb "raftstore/proto"
	"raftstore/raft"
)

type Server struct {
	pb.UnimplementedRaftServiceServer

	Node *raft.Node
}

// NewRaftServer creates a new gRPC Raft server.
func NewRaftServer(node *raft.Node) *Server {
	return &Server{
		Node: node,
	}
}

// RequestVote forwards the vote request to the Raft node.
func (s *Server) RequestVote(
	ctx context.Context,
	req *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {

	return s.Node.RequestVote(ctx, req)
}

// AppendEntries forwards heartbeats and log replication
// requests to the Raft node.
func (s *Server) AppendEntries(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {

	return s.Node.AppendEntries(ctx, req)
}

// Put forwards a client PUT request to the Raft node.
func (s *Server) Put(
	ctx context.Context,
	req *pb.PutRequest,
) (*pb.PutResponse, error) {

	return s.Node.Put(ctx, req)
}

// Get forwards a client GET request to the Raft node.
func (s *Server) Get(
	ctx context.Context,
	req *pb.GetRequest,
) (*pb.GetResponse, error) {

	return s.Node.Get(ctx, req)
}

// Delete forwards a client DELETE request to the Raft node.
func (s *Server) Delete(
	ctx context.Context,
	req *pb.DeleteRequest,
) (*pb.DeleteResponse, error) {

	return s.Node.Delete(ctx, req)
}
