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
// The ValuePointer returned by Append points to the beginning
// of the value bytes, not the record header.
type ValueLog struct {
	mu sync.RWMutex

	file *os.File
	path string

	nextOffset uint64
}

// ValueLogRecord describes one record physically stored
// in the ValueLog.
type ValueLogRecord struct {
	RecordOffset uint64
	ValueOffset  uint64
	Length       uint64
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

// Scan returns all valid records currently stored in the ValueLog.
//
// This does not modify the file.
func (v *ValueLog) Scan() ([]ValueLogRecord, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.file == nil {
		return nil, fmt.Errorf(
			"value log is closed",
		)
	}

	info, err := v.file.Stat()
	if err != nil {
		return nil, fmt.Errorf(
			"stat value log during scan: %w",
			err,
		)
	}

	fileSize := uint64(info.Size())

	var records []ValueLogRecord

	offset := uint64(0)

	for offset < fileSize {
		// A complete record must at least contain
		// the 8-byte length header.
		if fileSize-offset < valueLogHeaderSize {
			return nil, fmt.Errorf(
				"truncated value log header at offset %d",
				offset,
			)
		}

		header := make([]byte, valueLogHeaderSize)

		n, err := v.file.ReadAt(
			header,
			int64(offset),
		)

		if err != nil && err != io.EOF {
			return nil, fmt.Errorf(
				"read value log header at offset %d: %w",
				offset,
				err,
			)
		}

		if n != valueLogHeaderSize {
			return nil, fmt.Errorf(
				"incomplete value log header at offset %d",
				offset,
			)
		}

		length := binary.LittleEndian.Uint64(header)

		valueOffset := offset + valueLogHeaderSize

		// Check for overflow.
		if length > ^uint64(0)-valueOffset {
			return nil, fmt.Errorf(
				"invalid value length at offset %d",
				offset,
			)
		}

		nextOffset := valueOffset + length

		// Record extends beyond the file.
		if nextOffset > fileSize {
			return nil, fmt.Errorf(
				"truncated value log record at offset %d",
				offset,
			)
		}

		records = append(
			records,
			ValueLogRecord{
				RecordOffset: offset,
				ValueOffset:  valueOffset,
				Length:       length,
			},
		)

		offset = nextOffset
	}

	return records, nil
}

// Size returns the current ValueLog size.
func (v *ValueLog) Size() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()

	return v.nextOffset
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
