package metrics

import (
	"fmt"
	"os"
	"path/filepath"

	"raftstore/storage"
)

// StorageMetrics describes physical and logical storage usage.
type StorageMetrics struct {
	RocksDBBytes  uint64
	ValueLogBytes uint64

	TotalPhysicalBytes uint64
	LogicalBytes       uint64

	StorageOverheadBytes uint64
	StorageOverheadRatio float64

	ReclaimableBytes uint64
	LiveBytes        uint64
	ObsoleteBytes    uint64
}

// CollectStorageMetrics collects storage usage from a node's
// data directory and separated storage.
func CollectStorageMetrics(
	dataDir string,
	store *storage.SeparatedKVStore,
) (StorageMetrics, error) {
	if store == nil {
		return StorageMetrics{}, fmt.Errorf(
			"storage is nil",
		)
	}

	rocksDBPath := filepath.Join(
		dataDir,
		"rocksdb",
	)

	valueLogPath := filepath.Join(
		dataDir,
		"values.log",
	)

	rocksDBBytes, err := directorySize(rocksDBPath)
	if err != nil {
		return StorageMetrics{}, fmt.Errorf(
			"calculate RocksDB size: %w",
			err,
		)
	}

	valueLogBytes, err := fileSize(valueLogPath)
	if err != nil {
		return StorageMetrics{}, fmt.Errorf(
			"calculate ValueLog size: %w",
			err,
		)
	}

	gcStats, err := store.AnalyzeGC()
	if err != nil {
		return StorageMetrics{}, fmt.Errorf(
			"analyze storage for metrics: %w",
			err,
		)
	}

	logicalBytes := gcStats.LiveBytes

	totalPhysicalBytes := rocksDBBytes + valueLogBytes

	var overheadBytes uint64

	if totalPhysicalBytes >= logicalBytes {
		overheadBytes =
			totalPhysicalBytes - logicalBytes
	}

	var overheadRatio float64

	if logicalBytes > 0 {
		overheadRatio =
			float64(overheadBytes) /
				float64(logicalBytes)
	}

	return StorageMetrics{
		RocksDBBytes:         rocksDBBytes,
		ValueLogBytes:        valueLogBytes,
		TotalPhysicalBytes:   totalPhysicalBytes,
		LogicalBytes:         logicalBytes,
		StorageOverheadBytes: overheadBytes,
		StorageOverheadRatio: overheadRatio,
		ReclaimableBytes:     gcStats.ReclaimableBytes,
		LiveBytes:            gcStats.LiveBytes,
		ObsoleteBytes:        gcStats.ObsoleteBytes,
	}, nil
}

// fileSize returns the size of a file.
func fileSize(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}

	return uint64(info.Size()), nil
}

// directorySize recursively calculates directory size.
func directorySize(path string) (uint64, error) {
	var total uint64

	err := filepath.Walk(
		path,
		func(
			currentPath string,
			info os.FileInfo,
			err error,
		) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			total += uint64(info.Size())

			return nil
		},
	)

	if err != nil {
		return 0, err
	}

	return total, nil
}
