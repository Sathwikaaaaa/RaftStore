package controller

import (
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"raftstore/api"
	"raftstore/raft"
)

type MLController struct {
	server *api.Server
	node   *raft.Node
}

func NewMLController(server *api.Server, node *raft.Node) *MLController {
	return &MLController{
		server: server,
		node:   node,
	}
}

func (c *MLController) Start(interval time.Duration) {
	log.Printf("Starting Dual-Model XGBoost Adaptive Controller (Interval: %v)", interval)
	ticker := time.NewTicker(interval)

	go func() {
		for range ticker.C {
			c.adapt()
		}
	}()
}

func (c *MLController) adapt() {
	c.node.Mu.Lock()
	if c.node.State != raft.Leader {
		c.node.Mu.Unlock()
		return
	}
	c.node.Mu.Unlock()

	snapshot := c.server.MetricsSnapshot()

	totalOps := "1000"
	throughput := fmt.Sprintf("%.2f", 500.0)
	avgLat := fmt.Sprintf("%.2f", 10.0)
	p99Lat := fmt.Sprintf("%.2f", 50.0)
	writeAmp := fmt.Sprintf("%.4f", snapshot.Storage.WriteAmplification)

	// Predict Batch Size
	cmdBatch := exec.Command("python", "ml/infer.py", "batch", totalOps, throughput, avgLat, p99Lat, writeAmp)
	outBatch, errBatch := cmdBatch.Output()
	recommendedBatch := 100
	if errBatch == nil {
		if val, err := strconv.Atoi(strings.TrimSpace(string(outBatch))); err == nil {
			recommendedBatch = val
		}
	}

	// Predict Pipeline Depth
	cmdPipeline := exec.Command("python", "ml/infer.py", "pipeline", totalOps, throughput, avgLat, p99Lat, writeAmp)
	outPipeline, errPipeline := cmdPipeline.Output()
	recommendedPipeline := 1
	if errPipeline == nil {
		if val, err := strconv.Atoi(strings.TrimSpace(string(outPipeline))); err == nil {
			recommendedPipeline = val
		}
	}

	// Apply updates safely under lock
	c.node.Mu.Lock()
	oldBatch := c.node.RepConfig.MaxBatchSize
	oldPipeline := c.node.RepConfig.MaxPipelineDepth

	updated := false
	if oldBatch != recommendedBatch {
		c.node.RepConfig.MaxBatchSize = recommendedBatch
		updated = true
	}
	if oldPipeline != recommendedPipeline {
		c.node.RepConfig.MaxPipelineDepth = recommendedPipeline
		updated = true
	}

	if updated {
		log.Printf("[ML Controller] Adapted Config -> MaxBatchSize: %d -> %d, MaxPipelineDepth: %d -> %d (WriteAmp: %s)",
			oldBatch, recommendedBatch, oldPipeline, recommendedPipeline, writeAmp)
	}
	c.node.Mu.Unlock()
}