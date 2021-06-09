package car

import (
	"encoding/binary"
	"io"
)

const HeaderBytesLen uint64 = 32

var (
	// The fixed prefix of a CAR v2, signalling the version number to previois versoions for graceful fail over.
	PrefixBytes = []byte{
		0x0a,                                     // unit(10)
		0xa1,                                     // map(1)
		0x67,                                     // string(7)
		0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, // "version"
		0x02, // uint(2)
	}
	// The length of the CAR v2 prefix, i.e. 11 bytes.
	PrefixBytesLen = uint64(len(PrefixBytes))
	// Reserved 128 bits space to capture future characteristics of CAR v2 such as order, duplication, etc.
	EmptyCharacteristics = Characteristics(make([]byte, 16))
)

type (
	// Header represents the CAR v2 header/pragma.
	Header struct {
		// 128-bit characteristics of this CAR v2 file, such as order, deduplication, etc. Reserved for future use.
		Characteristics Characteristics
		// The length of CAR v1 bytes.
		CarV1Len uint64
		// The offset from the beginning of the file at which the CAR v2 index begins.
		IndexOffset uint64
	}
	Characteristics []byte
)

// NewHeader instantiates a new CAR v2 header, given the byte length of a CAR v1.
func NewHeader(carV1Len uint64) *Header {
	return &Header{
		Characteristics: EmptyCharacteristics,
		CarV1Len:        carV1Len,
	}
}

// WithPadding sets the index offset from the beginning of the file for this header and returns the
// header for convenient chained calls.
// The index offset is calculated as the sum of PrefixBytesLen, HeaderBytesLen,
// Header#CarV1Len, and the given padding.
func (h *Header) WithPadding(padding uint64) *Header {
	h.IndexOffset = PrefixBytesLen + HeaderBytesLen + h.CarV1Len + padding
	return h
}

// Marshal serializes this header to bytes and writes the bytes using the given io.Writer.
func (h *Header) Marshal(w io.Writer) (err error) {
	chars := h.Characteristics
	if chars == nil {
		chars = EmptyCharacteristics
	}
	_, err = w.Write(chars)
	if err != nil {
		return
	}
	err = binary.Write(w, binary.LittleEndian, h.CarV1Len)
	if err != nil {
		return
	}
	return binary.Write(w, binary.LittleEndian, h.IndexOffset)
}
