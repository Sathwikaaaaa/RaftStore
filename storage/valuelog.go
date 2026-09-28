package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

const valueLogHeaderSize = 8

// ValueLog stores values sequentially in an append-only file.
//
// Record format:
//
//	[8 bytes value length][value bytes]
//
// The returned offset points to the beginning of the value bytes.
type ValueLog struct {
	mu sync.RWMutex

	file *os.File
	path string

	nextOffset uint64
}

// NewValueLog opens or creates a ValueLog.
func NewValueLog(path string) (*ValueLog, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR,
		0644,
	)

	if err != nil {
		return nil, fmt.Errorf(
			"open value log: %w",
			err,
		)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()

		return nil, fmt.Errorf(
			"stat value log: %w",
			err,
		)
	}

	return &ValueLog{
		file:       file,
		path:       path,
		nextOffset: uint64(info.Size()),
	}, nil
}

// Append appends a value to the ValueLog.
func (v *ValueLog) Append(
	value []byte,
) (uint64, uint64, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.file == nil {
		return 0, 0, fmt.Errorf(
			"value log is closed",
		)
	}

	recordOffset := v.nextOffset

	header := make([]byte, valueLogHeaderSize)

	binary.LittleEndian.PutUint64(
		header,
		uint64(len(value)),
	)

	if _, err := v.file.WriteAt(
		header,
		int64(recordOffset),
	); err != nil {
		return 0, 0, fmt.Errorf(
			"write value log header: %w",
			err,
		)
	}

	valueOffset := recordOffset + valueLogHeaderSize

	if _, err := v.file.WriteAt(
		value,
		int64(valueOffset),
	); err != nil {
		return 0, 0, fmt.Errorf(
			"write value log data: %w",
			err,
		)
	}

	v.nextOffset =
		valueOffset + uint64(len(value))

	return valueOffset, uint64(len(value)), nil
}

// Read reads a value from the ValueLog.
func (v *ValueLog) Read(
	offset uint64,
	length uint64,
) ([]byte, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.file == nil {
		return nil, fmt.Errorf(
			"value log is closed",
		)
	}

	if length == 0 {
		return []byte{}, nil
	}

	value := make([]byte, length)

	n, err := v.file.ReadAt(
		value,
		int64(offset),
	)

	if err != nil && err != io.EOF {
		return nil, fmt.Errorf(
			"read value log: %w",
			err,
		)
	}

	if uint64(n) != length {
		return nil, fmt.Errorf(
			"incomplete value log read: expected %d bytes, got %d",
			length,
			n,
		)
	}

	return value, nil
}

// Sync flushes the ValueLog to disk.
func (v *ValueLog) Sync() error {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.file == nil {
		return fmt.Errorf(
			"value log is closed",
		)
	}

	if err := v.file.Sync(); err != nil {
		return fmt.Errorf(
			"sync value log: %w",
			err,
		)
	}

	return nil
}

// Size returns the current ValueLog size.
func (v *ValueLog) Size() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()

	return v.nextOffset
}

// Close flushes and closes the ValueLog.
func (v *ValueLog) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.file == nil {
		return nil
	}

	if err := v.file.Sync(); err != nil {
		return fmt.Errorf(
			"sync value log before close: %w",
			err,
		)
	}

	err := v.file.Close()

	v.file = nil

	return err
}
