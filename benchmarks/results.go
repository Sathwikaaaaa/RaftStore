package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type LatencyStats struct {
	TotalOperations   int
	SuccessfulOps     int
	FailedOps         int
	TotalDuration     time.Duration
	ThroughputOpsSec  float64
	AvgLatencyMs      float64
	P50LatencyMs      float64
	P95LatencyMs      float64
	P99LatencyMs      float64
	MinLatencyMs      float64
	MaxLatencyMs      float64
}

func CalculateStats(latencies []time.Duration, failed int, duration time.Duration) LatencyStats {
	total := len(latencies)
	success := total
	totalOps := success + failed

	stats := LatencyStats{
		TotalOperations:  totalOps,
		SuccessfulOps:    success,
		FailedOps:        failed,
		TotalDuration:    duration,
		ThroughputOpsSec: float64(success) / duration.Seconds(),
	}

	if success == 0 {
		return stats
	}

	// Convert to float64 milliseconds and sort
	msLatencies := make([]float64, success)
	var sum float64
	min := float64(latencies[0].Milliseconds())
	max := min

	for i, l := range latencies {
		ms := float64(l.Microseconds()) / 1000.0
		msLatencies[i] = ms
		sum += ms
		if ms < min {
			min = ms
		}
		if ms > max {
			max = ms
		}
	}

	sort.Float64s(msLatencies)

	stats.AvgLatencyMs = sum / float64(success)
	stats.MinLatencyMs = min
	stats.MaxLatencyMs = max
	stats.P50LatencyMs = msLatencies[int(float64(success)*0.50)]
	stats.P95LatencyMs = msLatencies[int(float64(success)*0.95)]
	stats.P99LatencyMs = msLatencies[int(float64(success)*0.99)]

	return stats
}

func SaveResultsToCSV(filename string, stats LatencyStats, config WorkloadConfig) error {
	resultsDir := "results"
	if err := os.MkdirAll(resultsDir, 0755); err != nil {
		return err
	}

	filePath := filepath.Join(resultsDir, filename)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write Header
	writer.Write([]string{
		"timestamp", "workload_type", "total_ops", "successful_ops", "failed_ops",
		"duration_sec", "throughput_ops_sec", "avg_latency_ms", "p50_latency_ms",
		"p95_latency_ms", "p99_latency_ms", "min_latency_ms", "max_latency_ms",
	})

	// Write Data Row
	writer.Write([]string{
		time.Now().Format(time.RFC3339),
		config.WorkloadType,
		fmt.Sprintf("%d", stats.TotalOperations),
		fmt.Sprintf("%d", stats.SuccessfulOps),
		fmt.Sprintf("%d", stats.FailedOps),
		fmt.Sprintf("%.2f", stats.TotalDuration.Seconds()),
		fmt.Sprintf("%.2f", stats.ThroughputOpsSec),
		fmt.Sprintf("%.2f", stats.AvgLatencyMs),
		fmt.Sprintf("%.2f", stats.P50LatencyMs),
		fmt.Sprintf("%.2f", stats.P95LatencyMs),
		fmt.Sprintf("%.2f", stats.P99LatencyMs),
		fmt.Sprintf("%.2f", stats.MinLatencyMs),
		fmt.Sprintf("%.2f", stats.MaxLatencyMs),
	})

	fmt.Printf("\n[Benchmark] Results successfully saved to %s\n", filePath)
	return nil
}