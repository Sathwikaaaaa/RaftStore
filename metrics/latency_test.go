package metrics

import (
	"testing"
	"time"
)

func TestLatencyRecorder(t *testing.T) {
	recorder := NewLatencyRecorder()

	recorder.Record(10 * time.Millisecond)
	recorder.Record(20 * time.Millisecond)
	recorder.Record(30 * time.Millisecond)
	recorder.Record(40 * time.Millisecond)
	recorder.Record(50 * time.Millisecond)

	stats := recorder.Stats()

	if stats.Count != 5 {
		t.Fatalf(
			"expected 5 samples, got %d",
			stats.Count,
		)
	}

	if stats.Average != 30*time.Millisecond {
		t.Fatalf(
			"expected average 30ms, got %v",
			stats.Average,
		)
	}

	if stats.Min != 10*time.Millisecond {
		t.Fatalf(
			"expected min 10ms, got %v",
			stats.Min,
		)
	}

	if stats.Max != 50*time.Millisecond {
		t.Fatalf(
			"expected max 50ms, got %v",
			stats.Max,
		)
	}

	if stats.P50 != 30*time.Millisecond {
		t.Fatalf(
			"expected P50 30ms, got %v",
			stats.P50,
		)
	}
}
