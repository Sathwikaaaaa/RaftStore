package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type GCState string

const (
	GCStateIdle              GCState = "IDLE"
	GCStatePrepared          GCState = "PREPARED"
	GCStateCompacted         GCState = "COMPACTED"
	GCStateMetadataCommitted GCState = "METADATA_COMMITTED"
)

type GCManifest struct {
	State GCState `json:"state"`

	OldValueLogPath string `json:"old_value_log_path"`
	NewValueLogPath string `json:"new_value_log_path"`

	UpdatedKeys int `json:"updated_keys"`
}

func manifestPath(dataDir string) string {
	return filepath.Join(dataDir, "gc_manifest.json")
}

func writeGCManifest(dataDir string, manifest GCManifest) error {
	path := manifestPath(dataDir)

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal GC manifest: %w", err)
	}

	tempPath := path + ".tmp"

	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("write temporary GC manifest: %w", err)
	}

	file, err := os.OpenFile(
		tempPath,
		os.O_RDWR,
		0644,
	)
	if err != nil {
		return fmt.Errorf("open temporary GC manifest: %w", err)
	}

	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync GC manifest: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("close temporary GC manifest: %w", err)
	}

	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace GC manifest: %w", err)
	}

	return nil
}

func readGCManifest(dataDir string) (GCManifest, error) {
	path := manifestPath(dataDir)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return GCManifest{
				State: GCStateIdle,
			}, nil
		}

		return GCManifest{}, fmt.Errorf(
			"read GC manifest: %w",
			err,
		)
	}

	var manifest GCManifest

	if err := json.Unmarshal(data, &manifest); err != nil {
		return GCManifest{}, fmt.Errorf(
			"decode GC manifest: %w",
			err,
		)
	}

	return manifest, nil
}

func clearGCManifest(dataDir string) error {
	path := manifestPath(dataDir)

	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf(
			"remove GC manifest: %w",
			err,
		)
	}

	return nil
}
