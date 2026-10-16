package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCompact(t *testing.T) {
	dataDir := t.TempDir()

	store, err := NewSeparatedKVStore(dataDir)
	if err != nil {
		t.Fatalf(
			"failed to create store: %v",
			err,
		)
	}

	// First version becomes obsolete later.
	if err := store.Put(
		"name",
		"Tanmai",
	); err != nil {
		t.Fatalf(
			"first PUT failed: %v",
			err,
		)
	}

	// Overwrite the same key.
	if err := store.Put(
		"name",
		"Tanmai-RaftStore",
	); err != nil {
		t.Fatalf(
			"second PUT failed: %v",
			err,
		)
	}

	// Another live value.
	if err := store.Put(
		"project",
		"RaftStore",
	); err != nil {
		t.Fatalf(
			"project PUT failed: %v",
			err,
		)
	}

	before, err := store.AnalyzeGC()
	if err != nil {
		t.Fatalf(
			"GC analysis before compaction failed: %v",
			err,
		)
	}

	if before.ObsoleteRecords != 1 {
		t.Fatalf(
			"expected 1 obsolete record before compaction, got %d",
			before.ObsoleteRecords,
		)
	}

	oldSize := before.TotalBytes

	// Perform compaction.
	if err := store.Compact(); err != nil {
		t.Fatalf(
			"compaction failed: %v",
			err,
		)
	}

	// Verify logical data.
	value, found, err := store.Get("name")
	if err != nil {
		t.Fatalf(
			"get name after compaction failed: %v",
			err,
		)
	}

	if !found {
		t.Fatal(
			"name missing after compaction",
		)
	}

	if value != "Tanmai-RaftStore" {
		t.Fatalf(
			"unexpected name after compaction: %q",
			value,
		)
	}

	value, found, err = store.Get("project")
	if err != nil {
		t.Fatalf(
			"get project after compaction failed: %v",
			err,
		)
	}

	if !found {
		t.Fatal(
			"project missing after compaction",
		)
	}

	if value != "RaftStore" {
		t.Fatalf(
			"unexpected project after compaction: %q",
			value,
		)
	}

	// Verify obsolete records are gone.
	after, err := store.AnalyzeGC()
	if err != nil {
		t.Fatalf(
			"GC analysis after compaction failed: %v",
			err,
		)
	}

	if after.ObsoleteRecords != 0 {
		t.Fatalf(
			"expected 0 obsolete records after compaction, got %d",
			after.ObsoleteRecords,
		)
	}

	if after.TotalRecords != 2 {
		t.Fatalf(
			"expected 2 records after compaction, got %d",
			after.TotalRecords,
		)
	}

	if after.TotalBytes >= oldSize {
		t.Fatalf(
			"expected compacted log to be smaller: old=%d new=%d",
			oldSize,
			after.TotalBytes,
		)
	}

	// Verify the physical file exists.
	valueLogPath := filepath.Join(
		dataDir,
		"values.log",
	)

	if _, err := os.Stat(valueLogPath); err != nil {
		t.Fatalf(
			"compacted value log does not exist: %v",
			err,
		)
	}

	// Close and reopen to verify persistence.
	if err := store.Close(); err != nil {
		t.Fatalf(
			"close after compaction failed: %v",
			err,
		)
	}

	store, err = NewSeparatedKVStore(dataDir)
	if err != nil {
		t.Fatalf(
			"reopen after compaction failed: %v",
			err,
		)
	}

	defer store.Close()

	value, found, err = store.Get("name")
	if err != nil {
		t.Fatalf(
			"get name after reopen failed: %v",
			err,
		)
	}

	if !found || value != "Tanmai-RaftStore" {
		t.Fatalf(
			"name incorrect after reopen: found=%v value=%q",
			found,
			value,
		)
	}

	value, found, err = store.Get("project")
	if err != nil {
		t.Fatalf(
			"get project after reopen failed: %v",
			err,
		)
	}

	if !found || value != "RaftStore" {
		t.Fatalf(
			"project incorrect after reopen: found=%v value=%q",
			found,
			value,
		)
	}
}
