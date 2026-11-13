package api

import (
	"encoding/json"
	"net/http"
	"time"

	"raftstore/raft"
)

type DashboardAPI struct {
	node      *raft.Node
	startTime time.Time
}

func NewDashboardAPI(node *raft.Node) *DashboardAPI {
	return &DashboardAPI{
		node:      node,
		startTime: time.Now(),
	}
}

// enableCORS allows the React frontend to poll the Go backend without browser security blocks
func (api *DashboardAPI) enableCORS(w *http.ResponseWriter) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
	(*w).Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	(*w).Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

// GET /api/status - Returns core node health and Raft state
func (api *DashboardAPI) StatusHandler(w http.ResponseWriter, r *http.Request) {
	api.enableCORS(&w)
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	api.node.Mu.Lock()
	state := api.node.State
	term := api.node.CurrentTerm
	logLen := len(api.node.Log)
	commitIdx := api.node.CommitIndex
	api.node.Mu.Unlock()

	stateStr := "Follower"
	if state == raft.Leader {
		stateStr = "Leader"
	} else if state == raft.Candidate {
		stateStr = "Candidate"
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"node_id":      api.node.ID,
		"state":        stateStr,
		"term":         term,
		"log_index":    logLen,
		"commit_index": commitIdx,
		"uptime_sec":   int(time.Since(api.startTime).Seconds()),
	})
}

// POST /api/fault - Triggers the exact chaos logic we built in tests
func (api *DashboardAPI) FaultHandler(w http.ResponseWriter, r *http.Request) {
	api.enableCORS(&w)
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	var req struct {
		Action string `json:"action"` // "crash" or "partition"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	api.node.Mu.Lock()
	defer api.node.Mu.Unlock()

	if req.Action == "crash" {
		api.node.State = raft.Follower
		api.node.CurrentTerm = 0
		select {
		case api.node.StopChan <- true:
		default:
		}
	} else if req.Action == "partition" {
		// Sever all client connections (Split-Brain injection)
		for peerID := range api.node.Clients {
			delete(api.node.Clients, peerID)
		}
	}

	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}

// POST /api/kv - Handles Key-Value operations directed at the cluster
func (api *DashboardAPI) KvHandler(w http.ResponseWriter, r *http.Request) {
	api.enableCORS(&w)
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	start := time.Now()
	var req struct {
		Op    string `json:"op"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	api.node.Mu.Lock()
	isLeader := (api.node.State == raft.Leader)
	leaderID := api.node.ID
	logIndex := len(api.node.Log)
	api.node.Mu.Unlock()

	if !isLeader {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Node is not the leader. Redirecting...",
			"node":    leaderID,
		})
		return
	}

	// NOTE: In your production build, you will pass req.Op, req.Key, req.Value 
	// into your actual Raft state machine here (e.g., api.node.Propose(...)).
	// For the dashboard UI wire-up, we are simulating the consensus delay.
	time.Sleep(12 * time.Millisecond)

	latency := time.Since(start).Milliseconds()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"op":         req.Op,
		"key":        req.Key,
		"value":      req.Value,
		"node":       leaderID,
		"log_index":  logIndex + 1,
		"latency_ms": latency,
	})
}