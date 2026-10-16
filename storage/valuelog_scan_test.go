package storage

import (
	"path/filepath"
	"testing"
)

func TestValueLogScan(t *testing.T) {
	path := filepath.Join(
		t.TempDir(),
		"values.log",
	)

	log, err := NewValueLog(path)
	if err != nil {
		t.Fatalf(
			"failed to create ValueLog: %v",
			err,
		)
	}

	defer log.Close()

	offset1, length1, err := log.Append(
		[]byte("Tanmai"),
	)
	if err != nil {
		t.Fatalf(
			"first append failed: %v",
			err,
		)
	}

	offset2, length2, err := log.Append(
		[]byte("RaftStore"),
	)
	if err != nil {
		t.Fatalf(
			"second append failed: %v",
			err,
		)
	}

	records, err := log.Scan()
	if err != nil {
		t.Fatalf(
			"scan failed: %v",
			err,
		)
	}

	if len(records) != 2 {
		t.Fatalf(
			"expected 2 records, got %d",
			len(records),
		)
	}

	if records[0].ValueOffset != offset1 {
		t.Fatalf(
			"first offset mismatch: expected %d, got %d",
			offset1,
			records[0].ValueOffset,
		)
	}

	if records[0].Length != length1 {
		t.Fatalf(
			"first length mismatch: expected %d, got %d",
			length1,
			records[0].Length,
		)
	}

	if records[1].ValueOffset != offset2 {
		t.Fatalf(
			"second offset mismatch: expected %d, got %d",
			offset2,
			records[1].ValueOffset,
		)
	}

	if records[1].Length != length2 {
		t.Fatalf(
			"second length mismatch: expected %d, got %d",
			length2,
			records[1].Length,
		)
	}
}
