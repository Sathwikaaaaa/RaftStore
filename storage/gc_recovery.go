package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

func recoverGC(
	dataDir string,
	manifest GCManifest,
) error {
	currentPath := filepath.Join(
		dataDir,
		"values.log",
	)

	switch manifest.State {

	case GCStatePrepared:
		// GC was started, but the compacted ValueLog was not
		// committed. The original ValueLog remains authoritative.

		if manifest.NewValueLogPath != "" {
			_ = os.Remove(manifest.NewValueLogPath)
		}

		return clearGCManifest(dataDir)

	case GCStateCompacted:
		// The compacted ValueLog is complete, but RocksDB
		// metadata was not committed.
		//
		// The original ValueLog remains authoritative.

		if manifest.NewValueLogPath != "" {
			_ = os.Remove(manifest.NewValueLogPath)
		}

		return clearGCManifest(dataDir)

	case GCStateMetadataCommitted:
		// RocksDB metadata has been updated to point to the
		// compacted ValueLog.
		//
		// Therefore the compacted ValueLog is now authoritative.

		if manifest.NewValueLogPath == "" {
			return fmt.Errorf(
				"metadata committed without a new ValueLog path",
			)
		}

		// If the compacted log still has its temporary name,
		// promote it to the canonical values.log name.
		if _, err := os.Stat(manifest.NewValueLogPath); err == nil {
			// Remove the old ValueLog first.
			if _, err := os.Stat(currentPath); err == nil {
				if err := os.Remove(currentPath); err != nil {
					return fmt.Errorf(
						"remove old ValueLog during recovery: %w",
						err,
					)
				}
			}

			if err := os.Rename(
				manifest.NewValueLogPath,
				currentPath,
			); err != nil {
				return fmt.Errorf(
					"promote compacted ValueLog during recovery: %w",
					err,
				)
			}
		} else if !os.IsNotExist(err) {
			return fmt.Errorf(
				"check compacted ValueLog during recovery: %w",
				err,
			)
		}

		// At this point values.log should exist.
		if _, err := os.Stat(currentPath); err != nil {
			return fmt.Errorf(
				"committed ValueLog is missing after recovery: %w",
				err,
			)
		}

		return clearGCManifest(dataDir)

	case GCStateIdle:
		return nil

	default:
		return fmt.Errorf(
			"unknown GC state: %q",
			manifest.State,
		)
	}
}
