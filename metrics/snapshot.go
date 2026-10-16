package metrics

import (
	"path/filepath"
	"time"

	"raftstore/raft"
)

type Snapshot struct {
	Timestamp time.Time

	NodeID string

	// Request metrics.
	Operations          uint64
	OperationsPerSecond float64

	Latency LatencyStats

	// Raft metrics.
	Raft RaftMetrics

	// Storage metrics.
	Storage StorageMetrics

	// Storage collection error, if any.
	StorageError string
}

type SnapshotCollector struct {
	Latency    *LatencyRecorder
	Throughput *ThroughputRecorder

	RaftTracker *RaftMetricsTracker
}

func NewSnapshotCollector(
	latency *LatencyRecorder,
	throughput *ThroughputRecorder,
	raftTracker *RaftMetricsTracker,
) *SnapshotCollector {

	return &SnapshotCollector{
		Latency:     latency,
		Throughput:  throughput,
		RaftTracker: raftTracker,
	}
}

func (c *SnapshotCollector) Collect(
	node *raft.Node,
) Snapshot {

	var latencyStats LatencyStats

	if c.Latency != nil {
		latencyStats = c.Latency.Stats()
	}

	var operations uint64

	var operationsPerSecond float64

	if c.Throughput != nil {
		operations = c.Throughput.Operations()

		operationsPerSecond =
			c.Throughput.OperationsPerSecond()
	}

	var raftMetrics RaftMetrics

	if c.RaftTracker != nil {
		raftMetrics =
			c.RaftTracker.Snapshot(node)
	}

	var storageMetrics StorageMetrics

	var storageError string

	nodeID := ""

	if node != nil {
		nodeID = node.ID
	}

	if node != nil && node.KV != nil {

		// Raft nodes store their persistent data under:
		//
		// data/<nodeID>/
		//
		// Therefore construct the same data directory here
		// instead of passing only the node ID.
		dataDir := filepath.Join(
			"data",
			node.ID,
		)

		var err error

		storageMetrics, err =
			CollectStorageMetrics(
				dataDir,
				node.KV,
			)

		if err != nil {
			storageError = err.Error()
		}
	}

	return Snapshot{
		Timestamp: time.Now(),

		NodeID: nodeID,

		Operations: operations,

		OperationsPerSecond: operationsPerSecond,

		Latency: latencyStats,

		Raft: raftMetrics,

		Storage: storageMetrics,

		StorageError: storageError,
	}
}
