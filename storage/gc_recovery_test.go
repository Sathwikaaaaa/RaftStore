package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGCRecoveryPrepared(t *testing.T) {
	dataDir := t.TempDir()

	oldPath := filepath.Join(dataDir, "values.log")
	newPath := filepath.Join(dataDir, "values.compact.log")

	if err := os.WriteFile(oldPath, []byte("old-data"), 0644); err != nil {
		t.Fatalf("write old ValueLog: %v", err)
	}

	if err := os.WriteFile(newPath, []byte("partial-new-data"), 0644); err != nil {
		t.Fatalf("write compacted ValueLog: %v", err)
	}

	manifest := GCManifest{
		State:           GCStatePrepared,
		OldValueLogPath: oldPath,
		NewValueLogPath: newPath,
	}

	if err := writeGCManifest(dataDir, manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	if err := recoverGC(dataDir, manifest); err != nil {
		t.Fatalf("recover prepared GC: %v", err)
	}

	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf(
			"old ValueLog should remain after PREPARED recovery: %v",
			err,
		)
	}

	if _, err := os.Stat(newPath); !os.IsNotExist(err) {
		t.Fatalf(
			"temporary ValueLog should be removed after PREPARED recovery",
		)
	}

	manifestPath := filepath.Join(dataDir, "gc_manifest.json")

	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Fatalf(
			"GC manifest should be removed after recovery",
		)
	}
}

func TestGCRecoveryCompacted(t *testing.T) {
	dataDir := t.TempDir()

	oldPath := filepath.Join(dataDir, "values.log")
	newPath := filepath.Join(dataDir, "values.compact.log")

	if err := os.WriteFile(oldPath, []byte("old-data"), 0644); err != nil {
		t.Fatalf("write old ValueLog: %v", err)
	}

	if err := os.WriteFile(newPath, []byte("compacted-data"), 0644); err != nil {
		t.Fatalf("write compacted ValueLog: %v", err)
	}

	manifest := GCManifest{
		State:           GCStateCompacted,
		OldValueLogPath: oldPath,
		NewValueLogPath: newPath,
	}

	if err := writeGCManifest(dataDir, manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	if err := recoverGC(dataDir, manifest); err != nil {
		t.Fatalf("recover compacted GC: %v", err)
	}

	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf(
			"old ValueLog should remain after COMPACTED recovery: %v",
			err,
		)
	}

	if _, err := os.Stat(newPath); !os.IsNotExist(err) {
		t.Fatalf(
			"uncommitted compacted ValueLog should be removed",
		)
	}
}

func TestGCRecoveryMetadataCommitted(t *testing.T) {
	dataDir := t.TempDir()

	oldPath := filepath.Join(dataDir, "values.log")
	newPath := filepath.Join(dataDir, "values.compact.log")

	if err := os.WriteFile(oldPath, []byte("old-data"), 0644); err != nil {
		t.Fatalf("write old ValueLog: %v", err)
	}

	if err := os.WriteFile(newPath, []byte("compacted-data"), 0644); err != nil {
		t.Fatalf("write compacted ValueLog: %v", err)
	}

	manifest := GCManifest{
		State:           GCStateMetadataCommitted,
		OldValueLogPath: oldPath,
		NewValueLogPath: newPath,
	}

	if err := writeGCManifest(dataDir, manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	if err := recoverGC(dataDir, manifest); err != nil {
		t.Fatalf(
			"recover metadata-committed GC: %v",
			err,
		)
	}

	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf(
			"current ValueLog should exist after recovery: %v",
			err,
		)
	}

	data, err := os.ReadFile(oldPath)
	if err != nil {
		t.Fatalf("read recovered ValueLog: %v", err)
	}

	if string(data) != "compacted-data" {
		t.Fatalf(
			"unexpected recovered ValueLog contents: %q",
			string(data),
		)
	}

	if _, err := os.Stat(newPath); !os.IsNotExist(err) {
		t.Fatalf(
			"temporary ValueLog should no longer exist after recovery",
		)
	}
}

func TestGCRecoveryIdle(t *testing.T) {
	dataDir := t.TempDir()

	manifest := GCManifest{
		State: GCStateIdle,
	}

	if err := recoverGC(dataDir, manifest); err != nil {
		t.Fatalf(
			"recover idle GC: %v",
			err,
		)
	}
}
