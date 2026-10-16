package metrics

import (
	"testing"
	"time"
)

func TestSnapshotBasic(t *testing.T) {

	latency :=
		NewLatencyRecorder()

	throughput :=
		NewThroughputRecorder()

	throughput.Start()

	latency.Record(
		10 * time.Millisecond,
	)

	latency.Record(
		20 * time.Millisecond,
	)

	throughput.Record()
	throughput.Record()

	tracker :=
		NewRaftMetricsTracker()

	collector :=
		NewSnapshotCollector(
			latency,
			throughput,
			tracker,
		)

	snapshot :=
		collector.Collect(nil)

	if snapshot.Operations != 2 {
		t.Fatalf(
			"expected 2 operations, got %d",
			snapshot.Operations,
		)
	}

	if snapshot.Latency.Count != 2 {
		t.Fatalf(
			"expected 2 latency samples, got %d",
			snapshot.Latency.Count,
		)
	}

	if snapshot.Timestamp.IsZero() {
		t.Fatal(
			"snapshot timestamp is zero",
		)
	}
}
