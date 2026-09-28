package storage

import (
	"encoding/binary"
	"fmt"
)

// ValuePointer identifies the location of a value in the ValueLog.
type ValuePointer struct {
	Offset uint64
	Length uint64
}

// EncodeValuePointer converts a ValuePointer into 16 bytes.
func EncodeValuePointer(pointer ValuePointer) []byte {
	data := make([]byte, 16)

	binary.LittleEndian.PutUint64(
		data[0:8],
		pointer.Offset,
	)

	binary.LittleEndian.PutUint64(
		data[8:16],
		pointer.Length,
	)

	return data
}

// DecodeValuePointer converts 16 bytes into a ValuePointer.
func DecodeValuePointer(data []byte) (ValuePointer, error) {
	if len(data) != 16 {
		return ValuePointer{}, fmt.Errorf(
			"invalid value pointer length: expected 16, got %d",
			len(data),
		)
	}

	return ValuePointer{
		Offset: binary.LittleEndian.Uint64(data[0:8]),
		Length: binary.LittleEndian.Uint64(data[8:16]),
	}, nil
}
