package storage

/*
#cgo CFLAGS: -IC:/vcpkg/installed/x64-windows/include
#cgo LDFLAGS: -LC:/vcpkg/installed/x64-windows/lib -lrocksdb -lrpcrt4 -lshlwapi -LC:/vcpkg/packages/zlib_x64-windows/lib -lz

#include <stdlib.h>
#include "rocksdb/c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// KVStore provides persistent key-value storage.
//
// Current backend:
//   RocksDB
//
// Future backend:
//   RocksDB key -> ValueLog offset
type KVStore struct {
	db *C.rocksdb_t
}

// NewKVStore opens or creates a RocksDB database.
//
// The database remains on disk after the process exits.
func NewKVStore(path string) (*KVStore, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	options := C.rocksdb_options_create()
	defer C.rocksdb_options_destroy(options)

	C.rocksdb_options_set_create_if_missing(options, 1)

	var errptr *C.char

	db := C.rocksdb_open(
		options,
		cPath,
		&errptr,
	)

	if db == nil {
		if errptr != nil {
			message := C.GoString(errptr)
			C.free(unsafe.Pointer(errptr))
			return nil, fmt.Errorf("rocksdb open failed: %s", message)
		}

		return nil, fmt.Errorf("rocksdb open failed")
	}

	return &KVStore{
		db: db,
	}, nil
}

// Close closes the RocksDB database.
func (s *KVStore) Close() {
	if s == nil || s.db == nil {
		return
	}

	C.rocksdb_close(s.db)
	s.db = nil
}

// Put stores or updates a key-value pair.
func (s *KVStore) Put(key, value string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("storage is closed")
	}

	writeOptions := C.rocksdb_writeoptions_create()
	defer C.rocksdb_writeoptions_destroy(writeOptions)

	cKey := C.CBytes([]byte(key))
	defer C.free(cKey)

	cValue := C.CBytes([]byte(value))
	defer C.free(cValue)

	var errptr *C.char

	C.rocksdb_put(
		s.db,
		writeOptions,
		(*C.char)(cKey),
		C.size_t(len(key)),
		(*C.char)(cValue),
		C.size_t(len(value)),
		&errptr,
	)

	if errptr != nil {
		message := C.GoString(errptr)
		C.free(unsafe.Pointer(errptr))
		return fmt.Errorf("rocksdb put failed: %s", message)
	}

	return nil
}

// Get retrieves a value.
//
// The second return value indicates whether the key exists.
func (s *KVStore) Get(key string) (string, bool, error) {
	if s == nil || s.db == nil {
		return "", false, fmt.Errorf("storage is closed")
	}

	readOptions := C.rocksdb_readoptions_create()
	defer C.rocksdb_readoptions_destroy(readOptions)

	cKey := C.CBytes([]byte(key))
	defer C.free(cKey)

	var valueLength C.size_t
	var errptr *C.char

	result := C.rocksdb_get(
		s.db,
		readOptions,
		(*C.char)(cKey),
		C.size_t(len(key)),
		&valueLength,
		&errptr,
	)

	if errptr != nil {
		message := C.GoString(errptr)
		C.free(unsafe.Pointer(errptr))
		return "", false, fmt.Errorf("rocksdb get failed: %s", message)
	}

	if result == nil {
		return "", false, nil
	}

	value := string(C.GoBytes(
		unsafe.Pointer(result),
		C.int(valueLength),
	))

	C.free(unsafe.Pointer(result))

	return value, true, nil
}

// Delete removes a key.
func (s *KVStore) Delete(key string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("storage is closed")
	}

	writeOptions := C.rocksdb_writeoptions_create()
	defer C.rocksdb_writeoptions_destroy(writeOptions)

	cKey := C.CBytes([]byte(key))
	defer C.free(cKey)

	var errptr *C.char

	C.rocksdb_delete(
		s.db,
		writeOptions,
		(*C.char)(cKey),
		C.size_t(len(key)),
		&errptr,
	)

	if errptr != nil {
		message := C.GoString(errptr)
		C.free(unsafe.Pointer(errptr))
		return fmt.Errorf("rocksdb delete failed: %s", message)
	}

	return nil
}