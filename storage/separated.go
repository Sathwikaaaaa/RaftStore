package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// SeparatedKVStore implements key-value separation.
//
// RocksDB stores:
//
//	key -> ValuePointer
//
// ValueLog stores:
//
//	actual value bytes.
type SeparatedKVStore struct {
	mu sync.RWMutex

	db       *KVStore
	valueLog *ValueLog
}

// NewSeparatedKVStore opens or creates the separated storage.
//
// Directory:
//
//	data/<node>/
//	├── rocksdb/
//	└── values.log
func NewSeparatedKVStore(dataDir string) (*SeparatedKVStore, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf(
			"create separated storage directory: %w",
			err,
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

	db, err := NewKVStore(rocksDBPath)
	if err != nil {
		return nil, fmt.Errorf(
			"open RocksDB: %w",
			err,
		)
	}

	valueLog, err := NewValueLog(valueLogPath)
	if err != nil {
		db.Close()

		return nil, fmt.Errorf(
			"open ValueLog: %w",
			err,
		)
	}

	return &SeparatedKVStore{
		db:       db,
		valueLog: valueLog,
	}, nil
}

// Put appends the actual value to the ValueLog and stores
// its offset and length in RocksDB.
func (s *SeparatedKVStore) Put(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db == nil || s.valueLog == nil {
		return fmt.Errorf("separated storage is closed")
	}

	offset, length, err := s.valueLog.Append([]byte(value))
	if err != nil {
		return fmt.Errorf(
			"append value: %w",
			err,
		)
	}

	// Ensure the value exists durably before publishing
	// its pointer in RocksDB.
	if err := s.valueLog.Sync(); err != nil {
		return fmt.Errorf(
			"sync value log: %w",
			err,
		)
	}

	pointer := ValuePointer{
		Offset: offset,
		Length: length,
	}

	metadata := EncodeValuePointer(pointer)

	if err := s.db.Put(
		key,
		string(metadata),
	); err != nil {
		return fmt.Errorf(
			"store value pointer: %w",
			err,
		)
	}

	return nil
}

// Get retrieves the value.
//
// RocksDB gives us the pointer.
// ValueLog gives us the actual value.
func (s *SeparatedKVStore) Get(
	key string,
) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.db == nil || s.valueLog == nil {
		return "", false, fmt.Errorf(
			"separated storage is closed",
		)
	}

	metadata, found, err := s.db.Get(key)

	if err != nil {
		return "", false, err
	}

	if !found {
		return "", false, nil
	}

	pointer, err := DecodeValuePointer(
		[]byte(metadata),
	)

	if err != nil {
		return "", false, fmt.Errorf(
			"decode value pointer: %w",
			err,
		)
	}

	value, err := s.valueLog.Read(
		pointer.Offset,
		pointer.Length,
	)

	if err != nil {
		return "", false, fmt.Errorf(
			"read value: %w",
			err,
		)
	}

	return string(value), true, nil
}

// Delete removes the key -> ValuePointer entry from RocksDB.
//
// The old ValueLog record is retained until garbage collection.
func (s *SeparatedKVStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db == nil || s.valueLog == nil {
		return fmt.Errorf(
			"separated storage is closed",
		)
	}

	return s.db.Delete(key)
}

// Close closes the ValueLog and RocksDB.
func (s *SeparatedKVStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db == nil && s.valueLog == nil {
		return nil
	}

	if s.valueLog != nil {
		if err := s.valueLog.Close(); err != nil {
			return fmt.Errorf(
				"close value log: %w",
				err,
			)
		}

		s.valueLog = nil
	}

	if s.db != nil {
		s.db.Close()
		s.db = nil
	}

	return nil
}
