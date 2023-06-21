package car

import (
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	internalio "github.com/ipld/go-car/v3/internal/io"
	"github.com/multiformats/go-varint"
)

type BlockReader interface {
	Version() uint64
	Roots() []cid.Cid
	Next() (cid.Cid, []byte, error)
}

type BlockReaderWithSkip interface {
	BlockReader
	SkipNext() (*BlockMetadata, error)
}

type BlockMetadata struct {
	cid.Cid
	Offset uint64
	Size   uint64
}

// BlockReader facilitates iteration over CAR blocks for both CARv1 and CARv2.
// See NewBlockReader
type blockReader struct {
	// The detected version of the CAR payload.
	version uint64
	// The roots of the CAR payload. May be empty.
	roots []cid.Cid

	// Used internally only, by BlockReader.Next during iteration over blocks.
	r          io.Reader
	offset     uint64
	readerSize int64
	opts       Options
}

// NewBlockReader instantiates a new BlockReader facilitating iteration over blocks in CARv1 or
// CARv2 payload. Upon instantiation, the version is automatically detected and exposed via
// BlockReader.Version. The root CIDs of the CAR payload are exposed via BlockReader.Roots
//
// See BlockReader.Next
func NewBlockReader(r io.Reader, opts ...Option) (BlockReaderWithSkip, error) {
	options := ApplyOptions(opts...)

	// Read CARv1 header or CARv2 pragma.
	// Both are a valid CARv1 header, therefore are read as such.
	pragmaOrV1Header := V1Header{}
	if _, err := pragmaOrV1Header.ReadFromChecked(r, options.MaxAllowedHeaderSize); err != nil {
		return nil, err
	}

	// Populate the block reader version and options.
	br := &blockReader{
		version: pragmaOrV1Header.Version,
		opts:    options,
	}

	// Expect either version 1 or 2.
	switch br.version {
	case 1:
		// If version is 1, r represents a CARv1.
		// Simply populate br.Roots and br.r without modifying r.
		br.roots = pragmaOrV1Header.Roots
		br.r = r
		br.readerSize = -1
		off, err := pragmaOrV1Header.WriteSize()
		if err != nil {
			return nil, err
		}
		br.offset = uint64(off)
	case 2:
		// If the version is 2:
		//  1. Read CARv2 specific header to locate the inner CARv1 data payload offset and size.
		//  2. Skip to the beginning of the inner CARv1 data payload.
		//  3. Re-initialize r as a LimitReader, limited to the size of the inner CARv1 payload.
		//  4. Read the header of inner CARv1 data payload via r to populate br.Roots.

		// Read CARv2-specific header.
		v2h := V2Header{}
		if _, err := v2h.ReadFrom(r); err != nil {
			return nil, err
		}

		// Skip to the beginning of inner CARv1 data payload.
		// Note, at this point the pragma and CARv1 header have been read.
		// An io.ReadSeeker is opportunistically constructed from the given io.Reader r.
		// The constructor does not take an initial offset, so we use Seek in io.SeekCurrent to
		// fast forward to the beginning of data payload by subtracting pragma and header size from
		// dataOffset.
		rs := internalio.ToByteReadSeeker(r)
		if _, err := rs.Seek(int64(v2h.DataOffset)-V2PragmaSize-V2HeaderSize, io.SeekCurrent); err != nil {
			return nil, err
		}
		br.offset = uint64(v2h.DataOffset)
		br.readerSize = int64(v2h.DataOffset + v2h.DataSize)

		// Set br.r to a LimitReader reading from r limited to dataSize.
		br.r = io.LimitReader(r, int64(v2h.DataSize))

		// Populate br.Roots by reading the inner CARv1 data payload header.
		header := V1Header{}
		if _, err := header.ReadFromChecked(br.r, options.MaxAllowedHeaderSize); err != nil {
			return nil, err
		}
		// Assert that the data payload header is exactly 1, i.e. the header represents a CARv1.
		if header.Version != 1 {
			return nil, fmt.Errorf("invalid data payload header version; expected 1, got %v", header.Version)
		}
		br.roots = header.Roots
	default:
		// Otherwise, error out with invalid version since only versions 1 or 2 are expected.
		return nil, fmt.Errorf("invalid car version: %d", br.version)
	}
	return br, nil
}

// Version returns the detected version of the CAR payload.
func (br *blockReader) Version() uint64 {
	return br.version
}

// Roots returns the root CIDs of the CAR payload.
func (br *blockReader) Roots() []cid.Cid {
	return br.roots
}

// Next iterates over blocks in the underlying CAR payload with an io.EOF error indicating the end
// is reached. Note, this function is forward-only; once the end has been reached it will always
// return io.EOF.
//
// When the payload represents a CARv1 the BlockReader.Next simply iterates over blocks until it
// reaches the end of the underlying io.Reader stream.
//
// As for CARv2 payload, the underlying io.Reader is read only up to the end of the last block.
// Note, in a case where ZeroLengthSectionAsEOF Option is enabled, io.EOF is returned
// immediately upon encountering a zero-length section without reading any further bytes from the
// underlying io.Reader.
func (br *blockReader) Next() (cid.Cid, []byte, error) {
	c, data, err := ReadSection(br.r, br.opts.ZeroLengthSectionAsEOF, br.opts.MaxAllowedSectionSize)
	if err != nil {
		return cid.Undef, nil, err
	}

	if !br.opts.TrustedCAR {
		hashed, err := c.Prefix().Sum(data)
		if err != nil {
			return cid.Undef, nil, err
		}

		if !hashed.Equals(c) {
			return cid.Undef, nil, fmt.Errorf("mismatch in content integrity, expected: %s, got: %s", c, hashed)
		}
	}

	ss := uint64(c.ByteLen()) + uint64(len(data))
	br.offset += uint64(varint.UvarintSize(ss)) + ss
	return c, data, nil
}

// SkipNext jumps over the next block, returning metadata about what it is (the CID, offset, and size).
// Like Next it will return an io.EOF once it has reached the end.
//
// If the underlying reader used by the BlockReader is actually a ReadSeeker, this method will attempt to
// seek over the underlying data rather than reading it into memory.
func (br *blockReader) SkipNext() (*BlockMetadata, error) {
	sctSize, err := lengthPrefixedReadSize(br.r, br.opts.ZeroLengthSectionAsEOF, br.opts.MaxAllowedSectionSize)
	if err != nil {
		return nil, err
	}

	if sctSize == 0 {
		_, _, err := cid.CidFromBytes([]byte{})
		return nil, err
	}

	cidSize, c, err := cid.CidFromReader(io.LimitReader(br.r, int64(sctSize)))
	if err != nil {
		return nil, err
	}

	blkSize := sctSize - uint64(cidSize)
	if brs, ok := br.r.(io.ReadSeeker); ok {
		// carv1 and we don't know the size, so work it out and cache it
		if br.readerSize == -1 {
			cur, err := brs.Seek(0, io.SeekCurrent)
			if err != nil {
				return nil, err
			}
			end, err := brs.Seek(0, io.SeekEnd)
			if err != nil {
				return nil, err
			}
			br.readerSize = end
			if _, err = brs.Seek(cur, io.SeekStart); err != nil {
				return nil, err
			}
		}
		// seek.
		finalOffset, err := brs.Seek(int64(blkSize), io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		if finalOffset != int64(br.offset)+int64(sctSize)+int64(varint.UvarintSize(sctSize)) {
			return nil, errors.New("unexpected length")
		}
		if finalOffset > br.readerSize {
			return nil, io.ErrUnexpectedEOF
		}
		br.offset = uint64(finalOffset)
		return &BlockMetadata{
			c,
			uint64(finalOffset) - sctSize - uint64(varint.UvarintSize(sctSize)),
			blkSize,
		}, nil
	}

	// read to end.
	readCnt, err := io.CopyN(io.Discard, br.r, int64(blkSize))
	if err != nil {
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if readCnt != int64(blkSize) {
		return nil, errors.New("unexpected length")
	}
	origOffset := br.offset
	br.offset += uint64(varint.UvarintSize(sctSize)) + sctSize

	return &BlockMetadata{
		c,
		origOffset,
		blkSize,
	}, nil
}
