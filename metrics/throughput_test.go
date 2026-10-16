package metrics

import (
	"testing"
	"time"
)

func TestThroughputRecorder(t *testing.T) {
	recorder := NewThroughputRecorder()

	recorder.Start()

	for i := 0; i < 100; i++ {
		recorder.Record()
	}

	time.Sleep(10 * time.Millisecond)

	duration := recorder.Stop()

	if duration <= 0 {
		t.Fatalf(
			"expected positive measurement duration",
		)
	}

	if recorder.Operations() != 100 {
		t.Fatalf(
			"expected 100 operations, got %d",
			recorder.Operations(),
		)
	}
}
