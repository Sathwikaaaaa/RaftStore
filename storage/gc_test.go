package storage

import (
	"testing"
)

func TestAnalyzeGC(t *testing.T) {
	dataDir := t.TempDir()

	store, err := NewSeparatedKVStore(dataDir)
	if err != nil {
		t.Fatalf(
			"failed to create store: %v",
			err,
		)
	}

	defer store.Close()

	// First version of the key.
	if err := store.Put(
		"name",
		"Tanmai",
	); err != nil {
		t.Fatalf(
			"first PUT failed: %v",
			err,
		)
	}

	// Second version of the same key.
	//
	// The first ValueLog record becomes obsolete because
	// RocksDB now points to the second record.
	if err := store.Put(
		"name",
		"Tanmai-RaftStore",
	); err != nil {
		t.Fatalf(
			"second PUT failed: %v",
			err,
		)
	}

	// Another live key.
	if err := store.Put(
		"project",
		"RaftStore",
	); err != nil {
		t.Fatalf(
			"project PUT failed: %v",
			err,
		)
	}

	stats, err := store.AnalyzeGC()
	if err != nil {
		t.Fatalf(
			"GC analysis failed: %v",
			err,
		)
	}

	// We created three ValueLog records:
	//
	// 1. name = Tanmai
	// 2. name = Tanmai-RaftStore
	// 3. project = RaftStore
	if stats.TotalRecords != 3 {
		t.Fatalf(
			"expected 3 total records, got %d",
			stats.TotalRecords,
		)
	}

	// Only records 2 and 3 are currently referenced.
	if stats.LiveRecords != 2 {
		t.Fatalf(
			"expected 2 live records, got %d",
			stats.LiveRecords,
		)
	}

	// Record 1 is obsolete.
	if stats.ObsoleteRecords != 1 {
		t.Fatalf(
			"expected 1 obsolete record, got %d",
			stats.ObsoleteRecords,
		)
	}

	if stats.ObsoleteBytes == 0 {
		t.Fatal(
			"expected obsolete bytes to be greater than zero",
		)
	}

	if stats.ReclaimableBytes != stats.ObsoleteBytes {
		t.Fatalf(
			"reclaimable bytes mismatch: expected %d, got %d",
			stats.ObsoleteBytes,
			stats.ReclaimableBytes,
		)
	}
}
