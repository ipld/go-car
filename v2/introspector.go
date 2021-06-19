package car

import (
	"bufio"
	"fmt"
	"github.com/ipfs/go-cid"
	carv1 "github.com/ipld/go-car"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"io"
)

// Introspection captures the result of an Introspect call.
type Introspection struct {
	// The version, either 1 or 2.
	Version uint64
	// The root CIDs if any.
	Roots []cid.Cid
	// Indicates whether the index is present.
	HasIndex bool
	// The offset at which the CAR v1 starts.
	// This value will be zero when version is 1.
	CarV1Offset uint64
	// The size of the CAR v1 payload.
	// If the version is 1 this value represents the total number of readable bytes.
	// If the version is 2 this value is fetched from the v2-specific header.
	CarV1Size uint64
	// The offset at which the index starts if it is present, otherwise 0.
	IndexOffset uint64
}

// Introspect introspects the given readable bytes and provides metadata about the characteristics
// and the version of CAR that r represents regardless of its version. This function is backward
// compatible; it supports both CAR v1 and v2.
// Returns error if r does not contain a valid CAR payload.
func Introspect(r io.ReaderAt) (*Introspection, error) {
	or := internalio.NewOffsetReader(r, 0)
	header, err := carv1.ReadHeader(bufio.NewReader(or))
	if err != nil {
		return nil, err
	}
	i := &Introspection{
		Version: header.Version,
	}
	switch header.Version {
	case 1:
		i.Roots = header.Roots
		or.SeekOffset(0)
		if i.CarV1Size, err = internalio.Size(or); err != nil {
			return i, err
		}
	case 2:
		or.SeekOffset(0)
		v2r, err := NewReader(or)
		if err != nil {
			return i, err
		}
		i.CarV1Offset = v2r.Header.CarV1Offset
		i.CarV1Size = v2r.Header.CarV1Size
		i.HasIndex = v2r.Header.Characteristics.HasIndex()
		i.IndexOffset = v2r.Header.IndexOffset
		if i.Roots, err = v2r.Roots(); err != nil {
			return i, err
		}
	default:
		return i, fmt.Errorf("unknown version: %v", i.Version)
	}
	return i, nil
}
