package storage

import (
	"path/filepath"
	"testing"
)

func TestSeparatedKVStore(t *testing.T) {
	dataDir := t.TempDir()

	store, err := NewSeparatedKVStore(dataDir)
	if err != nil {
		t.Fatalf("failed to create separated store: %v", err)
	}

	if err := store.Put("project", "RaftStore"); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	value, found, err := store.Get("project")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if !found {
		t.Fatal("expected key to exist")
	}

	if value != "RaftStore" {
		t.Fatalf(
			"unexpected value: expected %q, got %q",
			"RaftStore",
			value,
		)
	}

	if err := store.Delete("project"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	_, found, err = store.Get("project")
	if err != nil {
		t.Fatalf("get after delete failed: %v", err)
	}

	if found {
		t.Fatal("expected key to be deleted")
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestSeparatedKVStorePersistence(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "persistent-store")

	// First open.
	store, err := NewSeparatedKVStore(dataDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	if err := store.Put("name", "Tanmai"); err != nil {
		t.Fatalf("put name failed: %v", err)
	}

	if err := store.Put("project", "RaftStore"); err != nil {
		t.Fatalf("put project failed: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}

	// Reopen the same storage directory.
	store, err = NewSeparatedKVStore(dataDir)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store.Close()

	// Verify name survived restart.
	value, found, err := store.Get("name")
	if err != nil {
		t.Fatalf("get name after reopen failed: %v", err)
	}

	if !found {
		t.Fatal("name key was lost after reopen")
	}

	if value != "Tanmai" {
		t.Fatalf(
			"unexpected name after reopen: expected %q, got %q",
			"Tanmai",
			value,
		)
	}

	// Verify project survived restart.
	value, found, err = store.Get("project")
	if err != nil {
		t.Fatalf("get project after reopen failed: %v", err)
	}

	if !found {
		t.Fatal("project key was lost after reopen")
	}

	if value != "RaftStore" {
		t.Fatalf(
			"unexpected project after reopen: expected %q, got %q",
			"RaftStore",
			value,
		)
	}
}