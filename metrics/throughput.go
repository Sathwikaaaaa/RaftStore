package metrics

import (
	"sync"
	"time"
)

// ThroughputRecorder counts completed operations.
type ThroughputRecorder struct {
	mu sync.Mutex

	operations uint64

	started   bool
	startTime time.Time
}

// NewThroughputRecorder creates a throughput recorder.
func NewThroughputRecorder() *ThroughputRecorder {
	return &ThroughputRecorder{}
}

// Start begins the measurement interval.
func (r *ThroughputRecorder) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.operations = 0
	r.startTime = time.Now()
	r.started = true
}

// Record records one completed operation.
func (r *ThroughputRecorder) Record() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		return
	}

	r.operations++
}

// Stop ends the measurement interval.
func (r *ThroughputRecorder) Stop() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		return 0
	}

	duration := time.Since(r.startTime)
	r.started = false

	return duration
}

// Operations returns the number of completed operations.
func (r *ThroughputRecorder) Operations() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.operations
}

// OperationsPerSecond calculates throughput.
func (r *ThroughputRecorder) OperationsPerSecond() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		return 0
	}

	elapsed := time.Since(r.startTime)

	if elapsed <= 0 {
		return 0
	}

	return float64(r.operations) / elapsed.Seconds()
}
