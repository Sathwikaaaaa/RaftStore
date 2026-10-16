package metrics

import (
	"testing"

	"raftstore/storage"
)

func TestCollectStorageMetrics(t *testing.T) {
	dataDir := t.TempDir()

	store, err := storage.NewSeparatedKVStore(dataDir)
	if err != nil {
		t.Fatalf(
			"create separated storage: %v",
			err,
		)
	}

	defer store.Close()

	if err := store.Put(
		"key1",
		"value-one",
	); err != nil {
		t.Fatalf(
			"put key1: %v",
			err,
		)
	}

	if err := store.Put(
		"key2",
		"value-two",
	); err != nil {
		t.Fatalf(
			"put key2: %v",
			err,
		)
	}

	metrics, err := CollectStorageMetrics(
		dataDir,
		store,
	)

	if err != nil {
		t.Fatalf(
			"collect storage metrics: %v",
			err,
		)
	}

	if metrics.RocksDBBytes == 0 {
		t.Fatalf(
			"expected RocksDB storage to be non-zero",
		)
	}

	if metrics.ValueLogBytes == 0 {
		t.Fatalf(
			"expected ValueLog storage to be non-zero",
		)
	}

	if metrics.TotalPhysicalBytes == 0 {
		t.Fatalf(
			"expected total physical storage to be non-zero",
		)
	}

	if metrics.LiveBytes == 0 {
		t.Fatalf(
			"expected live storage bytes to be non-zero",
		)
	}

	if metrics.ReclaimableBytes != 0 {
		t.Fatalf(
			"expected no reclaimable bytes initially, got %d",
			metrics.ReclaimableBytes,
		)
	}
}
