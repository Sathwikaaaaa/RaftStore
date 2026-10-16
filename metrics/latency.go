package metrics

import (
	"sort"
	"sync"
	"time"
)

// LatencyRecorder collects request latency measurements.
type LatencyRecorder struct {
	mu      sync.Mutex
	samples []time.Duration
}

// NewLatencyRecorder creates a latency recorder.
func NewLatencyRecorder() *LatencyRecorder {
	return &LatencyRecorder{
		samples: make([]time.Duration, 0),
	}
}

// Record adds one latency measurement.
func (r *LatencyRecorder) Record(
	duration time.Duration,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.samples = append(r.samples, duration)
}

// Count returns the number of recorded samples.
func (r *LatencyRecorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.samples)
}

// Snapshot returns a copy of the recorded samples.
func (r *LatencyRecorder) Snapshot() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := make([]time.Duration, len(r.samples))
	copy(result, r.samples)

	return result
}

// Reset removes all recorded samples.
func (r *LatencyRecorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.samples = r.samples[:0]
}

// LatencyStats contains calculated latency statistics.
type LatencyStats struct {
	Count int

	Average time.Duration

	P50 time.Duration
	P95 time.Duration
	P99 time.Duration

	Min time.Duration
	Max time.Duration
}

// Stats calculates latency statistics from the current samples.
func (r *LatencyRecorder) Stats() LatencyStats {
	samples := r.Snapshot()

	if len(samples) == 0 {
		return LatencyStats{}
	}

	sort.Slice(
		samples,
		func(i, j int) bool {
			return samples[i] < samples[j]
		},
	)

	var total time.Duration

	for _, sample := range samples {
		total += sample
	}

	average := total / time.Duration(len(samples))

	return LatencyStats{
		Count:   len(samples),
		Average: average,
		P50:     percentile(samples, 0.50),
		P95:     percentile(samples, 0.95),
		P99:     percentile(samples, 0.99),
		Min:     samples[0],
		Max:     samples[len(samples)-1],
	}
}

// percentile calculates a percentile from sorted samples.
func percentile(
	samples []time.Duration,
	p float64,
) time.Duration {
	if len(samples) == 0 {
		return 0
	}

	if p <= 0 {
		return samples[0]
	}

	if p >= 1 {
		return samples[len(samples)-1]
	}

	index := int(float64(len(samples)-1) * p)

	return samples[index]
}
