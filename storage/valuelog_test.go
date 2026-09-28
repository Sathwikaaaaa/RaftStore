package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValueLogAppendRead(t *testing.T) {
	tempDir := t.TempDir()

	path := filepath.Join(
		tempDir,
		"values.log",
	)

	log, err := NewValueLog(path)

	if err != nil {
		t.Fatalf(
			"failed to create ValueLog: %v",
			err,
		)
	}

	value := []byte("Tanmai-RaftStore")

	offset, length, err := log.Append(value)

	if err != nil {
		t.Fatalf(
			"failed to append value: %v",
			err,
		)
	}

	if err := log.Sync(); err != nil {
		t.Fatalf(
			"failed to sync ValueLog: %v",
			err,
		)
	}

	got, err := log.Read(
		offset,
		length,
	)

	if err != nil {
		t.Fatalf(
			"failed to read value: %v",
			err,
		)
	}

	if string(got) != string(value) {
		t.Fatalf(
			"value mismatch: expected %q, got %q",
			value,
			got,
		)
	}

	if err := log.Close(); err != nil {
		t.Fatalf(
			"failed to close ValueLog: %v",
			err,
		)
	}

	// Verify the file actually exists.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf(
			"ValueLog file does not exist: %v",
			err,
		)
	}
}