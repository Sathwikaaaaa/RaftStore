package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// GCStats describes ValueLog utilization.
type GCStats struct {
	TotalRecords     uint64
	LiveRecords      uint64
	ObsoleteRecords  uint64
	TotalBytes       uint64
	LiveBytes        uint64
	ObsoleteBytes    uint64
	ReclaimableBytes uint64
}

// AnalyzeGC examines the ValueLog and determines which records
// are currently referenced by RocksDB.
//
// This method does NOT modify any data.
func (s *SeparatedKVStore) AnalyzeGC() (GCStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.db == nil || s.valueLog == nil {
		return GCStats{}, fmt.Errorf(
			"separated storage is closed",
		)
	}

	records, err := s.valueLog.Scan()
	if err != nil {
		return GCStats{}, fmt.Errorf(
			"scan value log: %w",
			err,
		)
	}

	stats := GCStats{
		TotalRecords: uint64(len(records)),
	}

	entries, err := s.db.ScanAll()
	if err != nil {
		return GCStats{}, fmt.Errorf(
			"scan RocksDB metadata: %w",
			err,
		)
	}

	livePointers := make(map[string]struct{})

	for _, metadata := range entries {
		pointer, err := DecodeValuePointer(
			[]byte(metadata),
		)

		if err != nil {
			return GCStats{}, fmt.Errorf(
				"decode RocksDB value pointer: %w",
				err,
			)
		}

		identity := fmt.Sprintf(
			"%d:%d",
			pointer.Offset,
			pointer.Length,
		)

		livePointers[identity] = struct{}{}
	}

	for _, record := range records {
		recordBytes := valueLogHeaderSize + record.Length

		stats.TotalBytes += recordBytes

		identity := fmt.Sprintf(
			"%d:%d",
			record.ValueOffset,
			record.Length,
		)

		if _, live := livePointers[identity]; live {
			stats.LiveRecords++
			stats.LiveBytes += recordBytes
		} else {
			stats.ObsoleteRecords++
			stats.ObsoleteBytes += recordBytes
		}
	}

	stats.ReclaimableBytes = stats.ObsoleteBytes

	return stats, nil
}

// Compact rewrites the ValueLog and retains only live values.
//
// The method does not change the logical key-value state.
//
// Before:
//
//	RocksDB:
//	    key1 -> old pointer
//	    key2 -> live pointer
//
//	ValueLog:
//	    old value ❌
//	    live value ✅
//
// After:
//
//	RocksDB:
//	    key1 -> new pointer
//	    key2 -> new pointer
//
//	ValueLog:
//	    only live values
//
// Compact rewrites the ValueLog and retains only live values.
//
// The operation uses a GC manifest to record progress so that
// startup recovery can detect an interrupted compaction.
//
// The logical key-value state is preserved.
func (s *SeparatedKVStore) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db == nil || s.valueLog == nil {
		return fmt.Errorf(
			"separated storage is closed",
		)
	}

	// ---------------------------------------------------------
	// 1. Determine storage paths.
	// ---------------------------------------------------------

	dataDir := filepath.Dir(s.valueLog.path)
	oldPath := s.valueLog.path

	tempPath := filepath.Join(
		dataDir,
		"values.compact.log",
	)

	// ---------------------------------------------------------
	// 2. Record that GC is starting.
	// ---------------------------------------------------------

	manifest := GCManifest{
		State:           GCStatePrepared,
		OldValueLogPath: oldPath,
		NewValueLogPath: tempPath,
		UpdatedKeys:     0,
	}

	if err := writeGCManifest(dataDir, manifest); err != nil {
		return fmt.Errorf(
			"write prepared GC manifest: %w",
			err,
		)
	}

	// Remove stale temporary log if one exists.
	_ = os.Remove(tempPath)

	// ---------------------------------------------------------
	// 3. Read current RocksDB metadata.
	// ---------------------------------------------------------

	entries, err := s.db.ScanAll()
	if err != nil {
		_ = clearGCManifest(dataDir)

		return fmt.Errorf(
			"scan RocksDB metadata: %w",
			err,
		)
	}

	// ---------------------------------------------------------
	// 4. Create temporary compacted ValueLog.
	// ---------------------------------------------------------

	tempLog, err := NewValueLog(tempPath)
	if err != nil {
		_ = clearGCManifest(dataDir)

		return fmt.Errorf(
			"create temporary value log: %w",
			err,
		)
	}

	// Any failure before COMPACTED should leave the original
	// ValueLog untouched.
	temporarySuccess := false

	defer func() {
		tempLog.Close()

		if !temporarySuccess {
			_ = os.Remove(tempPath)
		}
	}()

	// ---------------------------------------------------------
	// 5. Copy all currently live values.
	// ---------------------------------------------------------

	newPointers := make(map[string]ValuePointer)

	for key, metadata := range entries {
		oldPointer, err := DecodeValuePointer(
			[]byte(metadata),
		)

		if err != nil {
			return fmt.Errorf(
				"decode pointer for key %q: %w",
				key,
				err,
			)
		}

		value, err := s.valueLog.Read(
			oldPointer.Offset,
			oldPointer.Length,
		)

		if err != nil {
			return fmt.Errorf(
				"read value for key %q: %w",
				key,
				err,
			)
		}

		newOffset, newLength, err := tempLog.Append(
			value,
		)

		if err != nil {
			return fmt.Errorf(
				"append compacted value for key %q: %w",
				key,
				err,
			)
		}

		newPointers[key] = ValuePointer{
			Offset: newOffset,
			Length: newLength,
		}
	}

	// ---------------------------------------------------------
	// 6. Durably sync the compacted ValueLog.
	// ---------------------------------------------------------

	if err := tempLog.Sync(); err != nil {
		return fmt.Errorf(
			"sync compacted value log: %w",
			err,
		)
	}

	if err := tempLog.Close(); err != nil {
		return fmt.Errorf(
			"close compacted value log: %w",
			err,
		)
	}

	temporarySuccess = true

	// ---------------------------------------------------------
	// 7. Record that compacted data is ready.
	// ---------------------------------------------------------

	manifest.State = GCStateCompacted

	if err := writeGCManifest(dataDir, manifest); err != nil {
		return fmt.Errorf(
			"write compacted GC manifest: %w",
			err,
		)
	}

	// ---------------------------------------------------------
	// 8. Update RocksDB pointers.
	//
	// IMPORTANT:
	// The old ValueLog still exists at this point.
	// ---------------------------------------------------------

	for key, pointer := range newPointers {
		metadata := EncodeValuePointer(pointer)

		if err := s.db.Put(
			key,
			string(metadata),
		); err != nil {
			return fmt.Errorf(
				"update compacted pointer for key %q: %w",
				key,
				err,
			)
		}

		manifest.UpdatedKeys++
	}

	// ---------------------------------------------------------
	// 9. Record that metadata has been committed.
	// ---------------------------------------------------------

	manifest.State = GCStateMetadataCommitted

	if err := writeGCManifest(dataDir, manifest); err != nil {
		return fmt.Errorf(
			"write metadata-committed GC manifest: %w",
			err,
		)
	}

	// ---------------------------------------------------------
	// 10. Close the old ValueLog.
	// ---------------------------------------------------------

	if err := s.valueLog.Close(); err != nil {
		return fmt.Errorf(
			"close old value log: %w",
			err,
		)
	}

	// ---------------------------------------------------------
	// 11. Replace old ValueLog.
	// ---------------------------------------------------------

	if err := os.Remove(oldPath); err != nil {
		return fmt.Errorf(
			"remove old value log: %w",
			err,
		)
	}

	if err := os.Rename(
		tempPath,
		oldPath,
	); err != nil {
		return fmt.Errorf(
			"rename compacted value log: %w",
			err,
		)
	}

	// ---------------------------------------------------------
	// 12. Reopen compacted ValueLog.
	// ---------------------------------------------------------

	newLog, err := NewValueLog(oldPath)
	if err != nil {
		return fmt.Errorf(
			"reopen compacted value log: %w",
			err,
		)
	}

	s.valueLog = newLog

	// ---------------------------------------------------------
	// 13. GC completed.
	// ---------------------------------------------------------

	if err := clearGCManifest(dataDir); err != nil {
		return fmt.Errorf(
			"clear completed GC manifest: %w",
			err,
		)
	}

	return nil
}
