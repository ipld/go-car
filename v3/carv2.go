package car

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v3/index"
	internalio "github.com/ipld/go-car/v3/internal/io"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
	"golang.org/x/exp/mmap"
)

const (
	// V2PragmaSize is the size of the CARv2 pragma in bytes.
	V2PragmaSize = 11
	// V2HeaderSize is the fixed size of CARv2 header in number of bytes.
	V2HeaderSize = 40
	// V2CharacteristicsSize is the fixed size of Characteristics bitfield within CARv2 header in number of bytes.
	V2CharacteristicsSize = 16
)

// V2Pragma is the pragma of a CARv2, containing the version number.
// This is a valid CARv1 header, with version number of 2 and no root CIDs.
var V2Pragma = []byte{
	0x0a,                                     // unit(10)
	0xa1,                                     // map(1)
	0x67,                                     // string(7)
	0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, // "version"
	0x02, // uint(2)
}

type (
	// Header represents the CARv2 header.
	V2Header struct {
		// 128-bit characteristics of this CARv2 file, such as order, deduplication, etc. Reserved for future use.
		Characteristics Characteristics
		// The byte-offset from the beginning of the CARv2 to the first byte of the CARv1 data payload.
		DataOffset uint64
		// The byte-length of the CARv1 data payload.
		DataSize uint64
		// The byte-offset from the beginning of the CARv2 to the first byte of the index payload. This value may be 0 to indicate the absence of index data.
		IndexOffset uint64
	}
	// Characteristics is a bitfield placeholder for capturing the characteristics of a CARv2 such as order and determinism.
	Characteristics struct {
		Hi uint64
		Lo uint64
	}
)

// fullyIndexedCharPos is the position of Characteristics.Hi bit that specifies whether the index is a catalog af all CIDs or not.
const fullyIndexedCharPos = 7 // left-most bit

// WriteTo writes this characteristics to the given w.
func (c Characteristics) WriteTo(w io.Writer) (n int64, err error) {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[:8], c.Hi)
	binary.LittleEndian.PutUint64(buf[8:], c.Lo)
	written, err := w.Write(buf)
	return int64(written), err
}

func (c *Characteristics) ReadFrom(r io.Reader) (int64, error) {
	buf := make([]byte, V2CharacteristicsSize)
	read, err := io.ReadFull(r, buf)
	n := int64(read)
	if err != nil {
		return n, err
	}
	c.Hi = binary.LittleEndian.Uint64(buf[:8])
	c.Lo = binary.LittleEndian.Uint64(buf[8:])
	return n, nil
}

// IsFullyIndexed specifies whether the index of CARv2 represents a catalog of all CID segments.
// See StoreIdentityCIDs
func (c *Characteristics) IsFullyIndexed() bool {
	return isBitSet(c.Hi, fullyIndexedCharPos)
}

// SetFullyIndexed sets whether of CARv2 represents a catalog of all CID segments.
func (c *Characteristics) SetFullyIndexed(b bool) {
	if b {
		c.Hi = setBit(c.Hi, fullyIndexedCharPos)
	} else {
		c.Hi = unsetBit(c.Hi, fullyIndexedCharPos)
	}
}

func setBit(n uint64, pos uint) uint64 {
	n |= 1 << pos
	return n
}

func unsetBit(n uint64, pos uint) uint64 {
	mask := uint64(^(1 << pos))
	n &= mask
	return n
}

func isBitSet(n uint64, pos uint) bool {
	bit := n & (1 << pos)
	return bit > 0
}

// NewHeader instantiates a new CARv2 header, given the data size.
func NewV2Header(dataSize uint64) V2Header {
	header := V2Header{
		DataSize: dataSize,
	}
	header.DataOffset = V2PragmaSize + V2HeaderSize
	header.IndexOffset = header.DataOffset + dataSize
	return header
}

// WithIndexPadding sets the index offset from the beginning of the file for this header and returns
// the header for convenient chained calls.
// The index offset is calculated as the sum of V2PragmaSize, V2HeaderSize,
// Header.DataSize, and the given padding.
func (h V2Header) WithIndexPadding(padding uint64) V2Header {
	h.IndexOffset = h.IndexOffset + padding
	return h
}

// WithDataPadding sets the data payload byte-offset from the beginning of the file for this header
// and returns the header for convenient chained calls.
// The Data offset is calculated as the sum of V2PragmaSize, V2HeaderSize and the given padding.
// The call to this function also shifts the Header.IndexOffset forward by the given padding.
func (h V2Header) WithDataPadding(padding uint64) V2Header {
	h.DataOffset = V2PragmaSize + V2HeaderSize + padding
	h.IndexOffset = h.IndexOffset + padding
	return h
}

func (h V2Header) WithDataSize(size uint64) V2Header {
	h.DataSize = size
	h.IndexOffset = size + h.IndexOffset
	return h
}

// HasIndex indicates whether the index is present.
func (h V2Header) HasIndex() bool {
	return h.IndexOffset != 0
}

// WriteTo serializes this header as bytes and writes them using the given io.Writer.
func (h V2Header) WriteTo(w io.Writer) (n int64, err error) {
	wn, err := h.Characteristics.WriteTo(w)
	n += wn
	if err != nil {
		return
	}
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint64(buf[:8], h.DataOffset)
	binary.LittleEndian.PutUint64(buf[8:16], h.DataSize)
	binary.LittleEndian.PutUint64(buf[16:], h.IndexOffset)
	written, err := w.Write(buf)
	n += int64(written)
	return n, err
}

// ReadFrom populates fields of this header from the given r.
func (h *V2Header) ReadFrom(r io.Reader) (int64, error) {
	n, err := h.Characteristics.ReadFrom(r)
	if err != nil {
		return n, err
	}
	buf := make([]byte, 24)
	read, err := io.ReadFull(r, buf)
	n += int64(read)
	if err != nil {
		return n, err
	}
	dataOffset := binary.LittleEndian.Uint64(buf[:8])
	dataSize := binary.LittleEndian.Uint64(buf[8:16])
	indexOffset := binary.LittleEndian.Uint64(buf[16:])
	// Assert the data payload offset validity.
	// It must be at least 51 (<CARv2Pragma> + <CARv2Header>).
	if int64(dataOffset) < V2PragmaSize+V2HeaderSize {
		return n, fmt.Errorf("invalid data payload offset: %v", dataOffset)
	}
	// Assert the data size validity.
	// It must be larger than zero.
	// Technically, it should be at least 11 bytes (i.e. a valid CARv1 header with no roots) but
	// we let further parsing of the header to signal invalid data payload header.
	if int64(dataSize) <= 0 {
		return n, fmt.Errorf("invalid data payload size: %v", dataSize)
	}
	// Assert the index offset validity.
	if int64(indexOffset) < 0 {
		return n, fmt.Errorf("invalid index offset: %v", indexOffset)
	}
	h.DataOffset = dataOffset
	h.DataSize = dataSize
	h.IndexOffset = indexOffset
	return n, nil
}

// Reader represents a reader of CARv2.
type V2Reader struct {
	Header  V2Header
	Version uint64
	r       io.ReaderAt
	roots   []cid.Cid
	opts    Options
	closer  io.Closer
}

// OpenReader is a wrapper for NewReader which opens the file at path.
func OpenV2Reader(path string, opts ...Option) (*V2Reader, error) {
	f, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}

	r, err := NewV2Reader(f, opts...)
	if err != nil {
		return nil, err
	}

	r.closer = f
	return r, nil
}

// NewReader constructs a new reader that reads either CARv1 or CARv2 from the given r.
// Upon instantiation, the reader inspects the payload and provides appropriate read operations
// for both CARv1 and CARv2.
//
// Note that any other version other than 1 or 2 will result in an error. The caller may use
// Reader.Version to get the actual version r represents. In the case where r represents a CARv1
// Reader.Header will not be populated and is left as zero-valued.
func NewV2Reader(r io.ReaderAt, opts ...Option) (*V2Reader, error) {
	cr := &V2Reader{
		r: r,
	}
	cr.opts = ApplyOptions(opts...)

	or, err := internalio.NewOffsetReadSeeker(r, 0)
	if err != nil {
		return nil, err
	}
	cr.Version, err = ReadVersion(or, cr.opts.MaxAllowedHeaderSize)
	if err != nil {
		return nil, err
	}

	switch cr.Version {
	case 1:
	case 2:
		if err := cr.readV2Header(); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("invalid car version: %d", cr.Version)
	}

	return cr, nil
}

// Roots returns the root CIDs.
// The root CIDs are extracted lazily from the data payload header.
func (r *V2Reader) Roots() ([]cid.Cid, error) {
	if r.roots != nil {
		return r.roots, nil
	}
	dr, err := r.DataReader()
	if err != nil {
		return nil, err
	}
	header := V1Header{}
	if _, err := header.ReadFromChecked(dr, r.opts.MaxAllowedHeaderSize); err != nil {
		return nil, err
	}
	r.roots = header.Roots
	return r.roots, nil
}

func (r *V2Reader) readV2Header() (err error) {
	headerSection := io.NewSectionReader(r.r, V2PragmaSize, V2HeaderSize)
	_, err = r.Header.ReadFrom(headerSection)
	return
}

// PayloadReader implements both io.ReadSeeker and io.ReaderAt.
// It is the interface version of io.SectionReader, but note that the
// implementation is not guaranteed to be an io.SectionReader.
type PayloadReader interface {
	io.Reader
	io.Seeker
	io.ReaderAt
}

// DataReader provides a reader containing the data payload in CARv1 format.
func (r *V2Reader) DataReader() (PayloadReader, error) {
	if r.Version == 2 {
		return io.NewSectionReader(r.r, int64(r.Header.DataOffset), int64(r.Header.DataSize)), nil
	}
	return internalio.NewOffsetReadSeeker(r.r, 0)
}

// IndexReader provides an io.Reader containing the index for the data payload if the index is
// present. Otherwise, returns nil.
// Note, this function will always return nil if the backing payload represents a CARv1.
func (r *V2Reader) IndexReader() (io.Reader, error) {
	if r.Version == 1 || !r.Header.HasIndex() {
		return nil, nil
	}
	return internalio.NewOffsetReadSeeker(r.r, int64(r.Header.IndexOffset))
}

// Stats is returned by an Inspect() call
type Stats struct {
	Version        uint64
	Header         V2Header
	Roots          []cid.Cid
	RootsPresent   bool
	BlockCount     uint64
	CodecCounts    map[multicodec.Code]uint64
	MhTypeCounts   map[multicodec.Code]uint64
	AvgCidLength   uint64
	MaxCidLength   uint64
	MinCidLength   uint64
	AvgBlockLength uint64
	MaxBlockLength uint64
	MinBlockLength uint64
	IndexCodec     multicodec.Code
}

// Inspect does a quick scan of a CAR, performing basic validation of the format
// and returning a Stats object that provides a high-level description of the
// contents of the CAR.
// Inspect works for CARv1 and CARv2 contents. A CARv1 will return an
// uninitialized Header value.
//
// If validateBlockHash is true, all block data in the payload will be hashed
// and compared to the CID for that block and an error will return if there
// is a mismatch. If false, block data will be skipped over and not checked.
// Performing a full block hash validation is similar to using a BlockReader and
// calling Next over all blocks.
//
// Inspect will perform a basic check of a CARv2 index, where present, but this
// does not guarantee that the index is correct. Attempting to read index data
// from untrusted sources is not recommended. If required, further validation of
// an index can be performed by loading the index and performing a ForEach() and
// sanity checking that the offsets are within the data payload section of the
// CAR. However, re-generation of index data in this case is the recommended
// course of action.
//
// Beyond the checks performed by Inspect, a valid / good CAR is somewhat
// use-case dependent. Factors to consider include:
//
//   - Bad indexes, including incorrect offsets, duplicate entries, or other
//     faulty data. Indexes should be re-generated, regardless, if you need to use
//     them and have any reason to not trust the source.
//
//   - Blocks use codecs that your system doesn't have access to—which may mean
//     you can't traverse a DAG or use the contained data. Stats.CodecCounts
//     contains a list of codecs found in the CAR so this can be checked.
//
//   - CIDs use multihashes that your system doesn't have access to—which will
//     mean you can't validate block hashes are correct (using validateBlockHash
//     in this case will result in a failure). Stats.MhTypeCounts contains a
//     list of multihashes found in the CAR so this can be checked.
//
//   - The presence of IDENTITY CIDs, which may not be supported (or desired) by
//     the consumer of the CAR. Stats.CodecCounts can determine the presence
//     of IDENTITY CIDs.
//
//   - Roots: the number of roots, duplicates, and whether they are related to the
//     blocks contained within the CAR. Stats contains a list of Roots and a
//     RootsPresent bool so further checks can be performed.
//
//   - DAG completeness is not checked. Any properties relating to the DAG, or
//     DAGs contained within a CAR are the responsibility of the user to check.
func (r *V2Reader) Inspect(validateBlockHash bool) (Stats, error) {
	stats := Stats{
		Version:      r.Version,
		Header:       r.Header,
		CodecCounts:  make(map[multicodec.Code]uint64),
		MhTypeCounts: make(map[multicodec.Code]uint64),
	}

	var totalCidLength uint64
	var totalBlockLength uint64
	var minCidLength uint64 = math.MaxUint64
	var minBlockLength uint64 = math.MaxUint64

	dr, err := r.DataReader()
	if err != nil {
		return Stats{}, err
	}
	bdr := internalio.ToByteReader(dr)

	// read roots, not using Roots(), because we need the offset setup in the data trader
	header := V1Header{}
	if _, err := header.ReadFromChecked(dr, r.opts.MaxAllowedHeaderSize); err != nil {
		return Stats{}, err
	}
	stats.Roots = header.Roots
	var rootsPresentCount int
	rootsPresent := make([]bool, len(stats.Roots))

	// read block sections
	for {
		sectionLength, err := varint.ReadUvarint(bdr)
		if err != nil {
			if err == io.EOF {
				// if the length of bytes read is non-zero when the error is EOF then signal an unclean EOF.
				if sectionLength > 0 {
					return Stats{}, io.ErrUnexpectedEOF
				}
				// otherwise, this is a normal ending
				break
			}
			return Stats{}, err
		}
		if sectionLength == 0 && r.opts.ZeroLengthSectionAsEOF {
			// normal ending for this read mode
			break
		}
		if sectionLength > r.opts.MaxAllowedSectionSize {
			return Stats{}, ErrSectionTooLarge
		}

		// decode just the CID bytes
		cidLen, c, err := cid.CidFromReader(dr)
		if err != nil {
			return Stats{}, err
		}

		if sectionLength < uint64(cidLen) {
			// this case is handled different in the normal ReadNode() path since it
			// slurps in the whole section bytes and decodes CID from there - so an
			// error should come from a failing io.ReadFull
			return Stats{}, errors.New("section length shorter than CID length")
		}

		// is this a root block? (also account for duplicate root CIDs)
		if rootsPresentCount < len(stats.Roots) {
			for i, r := range stats.Roots {
				if !rootsPresent[i] && c == r {
					rootsPresent[i] = true
					rootsPresentCount++
				}
			}
		}

		cp := c.Prefix()
		codec := multicodec.Code(cp.Codec)
		count := stats.CodecCounts[codec]
		stats.CodecCounts[codec] = count + 1
		mhtype := multicodec.Code(cp.MhType)
		count = stats.MhTypeCounts[mhtype]
		stats.MhTypeCounts[mhtype] = count + 1

		blockLength := sectionLength - uint64(cidLen)

		if validateBlockHash {
			// Use multihash.SumStream to avoid having to copy the entire block content into memory.
			// The SumStream uses a buffered copy to write bytes into the hasher which will take
			// advantage of streaming hash calculation depending on the hash function.
			// TODO: introduce SumStream in go-cid to simplify the code here.
			blockReader := io.LimitReader(dr, int64(blockLength))
			mhl := cp.MhLength
			if mhtype == multicodec.Identity {
				mhl = -1
			}
			mh, err := multihash.SumStream(blockReader, cp.MhType, mhl)
			if err != nil {
				return Stats{}, err
			}
			var gotCid cid.Cid
			switch cp.Version {
			case 0:
				gotCid = cid.NewCidV0(mh)
			case 1:
				gotCid = cid.NewCidV1(cp.Codec, mh)
			default:
				return Stats{}, fmt.Errorf("invalid cid version: %d", cp.Version)
			}
			if !gotCid.Equals(c) {
				return Stats{}, fmt.Errorf("mismatch in content integrity, expected: %s, got: %s", c, gotCid)
			}
		} else {
			// otherwise, skip over it
			if _, err := dr.Seek(int64(blockLength), io.SeekCurrent); err != nil {
				return Stats{}, err
			}
		}

		stats.BlockCount++
		totalCidLength += uint64(cidLen)
		totalBlockLength += blockLength
		if uint64(cidLen) < minCidLength {
			minCidLength = uint64(cidLen)
		}
		if uint64(cidLen) > stats.MaxCidLength {
			stats.MaxCidLength = uint64(cidLen)
		}
		if blockLength < minBlockLength {
			minBlockLength = blockLength
		}
		if blockLength > stats.MaxBlockLength {
			stats.MaxBlockLength = blockLength
		}
	}

	stats.RootsPresent = len(stats.Roots) == rootsPresentCount
	if stats.BlockCount > 0 {
		stats.MinCidLength = minCidLength
		stats.MinBlockLength = minBlockLength
		stats.AvgCidLength = totalCidLength / stats.BlockCount
		stats.AvgBlockLength = totalBlockLength / stats.BlockCount
	}

	if stats.Version != 1 && stats.Header.HasIndex() {
		idxr, err := r.IndexReader()
		if err != nil {
			return Stats{}, err
		}
		stats.IndexCodec, err = index.ReadCodec(idxr)
		if err != nil {
			return Stats{}, err
		}
	}

	return stats, nil
}

// Close closes the underlying reader if it was opened by OpenReader.
func (r *V2Reader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

// ReadVersion reads the version from the initial bytes of a reader.
// This function accepts both CARv1 and CARv2 payloads.
func ReadVersion(r io.Reader, maxReadBytes uint64) (uint64, error) {
	header := V1Header{}
	if _, err := header.ReadFromUnchecked(r, maxReadBytes); err != nil {
		return 0, err
	}
	return header.Version, nil
}
