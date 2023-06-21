package testutil

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	car "github.com/ipld/go-car/v3"
	internalio "github.com/ipld/go-car/v3/internal/io"
)

type SimpleV1Reader struct {
	r                     io.Reader
	Header                *car.V1Header
	zeroLenAsEOF          bool
	maxAllowedSectionSize uint64
}

func NewV1Reader(r io.Reader, zeroLenAsEOF bool, maxAllowedHeaderSize uint64, maxAllowedSectionSize uint64) (*SimpleV1Reader, error) {
	if maxAllowedHeaderSize == 0 {
		maxAllowedHeaderSize = 32 << 20
	}
	if maxAllowedSectionSize == 0 {
		maxAllowedSectionSize = 8 << 20
	}
	ch := car.V1Header{}
	if _, err := ch.ReadFromChecked(r, maxAllowedHeaderSize); err != nil {
		return nil, err
	}

	if ch.Version != 1 {
		return nil, fmt.Errorf("invalid car version: %d", ch.Version)
	}

	return &SimpleV1Reader{
		r:                     r,
		Header:                &ch,
		zeroLenAsEOF:          zeroLenAsEOF,
		maxAllowedSectionSize: maxAllowedSectionSize,
	}, nil
}

func NewV1ReaderFromV2(r io.Reader, zeroLenAsEOF bool, maxAllowedHeaderSize uint64, maxAllowedSectionSize uint64) (*SimpleV1Reader, error) {
	var pragma [car.V2PragmaSize]byte
	_, err := io.ReadFull(r, pragma[:])
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(pragma[:], car.V2Pragma) {
		return nil, errors.New("invalid car pragma")
	}

	header := car.V2Header{}
	_, err = header.ReadFrom(r)
	if err != nil {
		return nil, err
	}

	var ra io.ReaderAt
	if _ra, ok := r.(io.ReaderAt); ok {
		ra = _ra
	} else if _rs, ok := r.(io.ReadSeeker); ok {
		ra = internalio.ToReaderAt(_rs)
	}

	return NewV1Reader(io.NewSectionReader(ra, int64(header.DataOffset), int64(header.DataSize)), zeroLenAsEOF, maxAllowedHeaderSize, maxAllowedSectionSize)
}

func (sv1r *SimpleV1Reader) Roots() []cid.Cid {
	return sv1r.Header.Roots
}

func (sv1r *SimpleV1Reader) Version() uint64 {
	return 1
}

func (sv1r *SimpleV1Reader) Next() (cid.Cid, []byte, error) {
	c, data, err := car.ReadSection(sv1r.r, sv1r.zeroLenAsEOF, sv1r.maxAllowedSectionSize)
	if err != nil {
		return cid.Undef, nil, err
	}

	hashed, err := c.Prefix().Sum(data)
	if err != nil {
		return cid.Undef, nil, err
	}

	if !hashed.Equals(c) {
		return cid.Undef, nil, fmt.Errorf("mismatch in content integrity, name: %s, data: %s", c, hashed)
	}

	return c, data, err
}

func (sv1r *SimpleV1Reader) SkipNext() (*car.BlockMetadata, error) {
	panic("unimplemented")
}
