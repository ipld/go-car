package car

import (
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	internalio "github.com/ipld/go-car/v3/internal/io"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/multiformats/go-varint"
)

// ErrSectionTooLarge is returned when the length of a section exceeds the
// maximum allowed size.
var ErrSectionTooLarge = errors.New("invalid section data, length of read beyond allowable maximum")

// ErrHeaderTooLarge is returned when the length of a header exceeds the
// maximum allowed size.
var ErrHeaderTooLarge = errors.New("invalid header data, length of read beyond allowable maximum")

// V1HeaderSchema is the IPLD schema for the CARv1 header.
//
// CarV1HeaderOrV2Pragma is a more relaxed form, and can parse {version:x} where
// roots are optional. This is typically useful for the {verison:2} CARv2
// pragma.
//
// CarV1Header is the strict form of the header, and requires roots to be
// present. This is compatible with the CARv1 specification.
const V1HeaderSchema = `
type CarV1HeaderOrV2Pragma struct {
	roots optional [&Any]
	# roots is _not_ optional for CarV1 but we defer that check within code to
	# gracefully handle the V2 case where it's just {version:X}
	version Int
}

type CarV1Header struct {
	roots [&Any]
	version Int
}
`

var v1HeaderPrototype schema.TypedPrototype
var v1HeaderOrPragmaPrototype schema.TypedPrototype

type V1Header struct {
	Roots   []cid.Cid
	Version uint64
}

// Matches checks whether two headers match.
// Two headers are considered matching if:
//  1. They have the same version number, and
//  2. They contain the same root CIDs in any order.
//
// Note, this function explicitly ignores the order of roots.
// If order of roots matter use reflect.DeepEqual instead.
func (h V1Header) Matches(other V1Header) bool {
	if h.Version != other.Version {
		return false
	}
	thisLen := len(h.Roots)
	if thisLen != len(other.Roots) {
		return false
	}
	// Headers with a single root are popular.
	// Implement a fast execution path for popular cases.
	if thisLen == 1 {
		return h.Roots[0].Equals(other.Roots[0])
	}

	// Check other contains all roots.
	// TODO: should this be optimised for cases where the number of roots are large since it has O(N^2) complexity?
	for _, r := range h.Roots {
		if !other.containsRoot(r) {
			return false
		}
	}
	return true
}

func (h *V1Header) containsRoot(root cid.Cid) bool {
	for _, r := range h.Roots {
		if r.Equals(root) {
			return true
		}
	}
	return false
}

func lengthPrefixedSize(d ...[]byte) int64 {
	var sum int64
	for _, s := range d {
		sum += int64(len(s))
	}
	s := int64(varint.UvarintSize(uint64(sum)))
	return sum + int64(s)
}

func lengthPrefixedReadSize(r io.Reader, zeroLenAsEOF bool, maxReadBytes uint64) (uint64, error) {
	l, err := varint.ReadUvarint(internalio.ToByteReader(r))
	if err != nil {
		// If the length of bytes read is non-zero when the error is EOF then signal an unclean EOF.
		if l > 0 && err == io.EOF {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	} else if l == 0 && zeroLenAsEOF {
		return 0, io.EOF
	}

	if l > maxReadBytes { // Don't OOM
		return 0, ErrSectionTooLarge
	}
	return l, nil
}

func lengthPrefixedRead(r io.Reader, zeroLenAsEOF bool, maxReadBytes uint64) ([]byte, error) {
	l, err := lengthPrefixedReadSize(r, zeroLenAsEOF, maxReadBytes)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	return buf, nil
}

// LengthPrefixedWrite writes the given data to the writer prefixed by the
// length of the data in bytes encoded as a varint. Multiple data slices can be
// passed in and they will be concatenated together.
//
// A standard CARv1 section concatenates the bytes of a CID and the bytes of the
// block data, e.g.: LengthPrefixedWrite(cid.Bytes(), data).
func LengthPrefixedWrite(w io.Writer, d ...[]byte) error {
	var sum uint64
	for _, s := range d {
		sum += uint64(len(s))
	}

	buf := make([]byte, 8)
	n := varint.PutUvarint(buf, sum)
	_, err := w.Write(buf[:n])
	if err != nil {
		return err
	}

	for _, s := range d {
		_, err = w.Write(s)
		if err != nil {
			return err
		}
	}

	return nil
}

// ReadSection reads a section from the given reader. It performs a length-
// prefixed read of the data and returns the CID and the data of the section.
func ReadSection(r io.Reader, zeroLenAsEOF bool, maxReadBytes uint64) (cid.Cid, []byte, error) {
	data, err := lengthPrefixedRead(r, zeroLenAsEOF, maxReadBytes)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	n, c, err := cid.CidFromBytes(data)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	return c, data[n:], nil
}

// ReadFromUnchecked populates fields of this header from the given r. If
// maxReadBytes is non-zero, it will return ErrHeaderTooLarge if the header is
// larger than maxReadBytes.
//
// This method does not fully validate the header. Use ReadFromChecked to
// validate the header's version and roots fields. This method will only
// validate according to the CarV1HeaderOrV2Pragma type in the V1HeaderSchema.
func (h *V1Header) ReadFromUnchecked(r io.Reader, maxReadBytes uint64) (int64, error) {
	cr := internalio.NewCountingReader(r)
	hb, err := lengthPrefixedRead(cr, false, maxReadBytes)
	if err != nil {
		if err == ErrSectionTooLarge {
			err = ErrHeaderTooLarge
		}
		return cr.Count(), err
	}

	node, err := ipld.DecodeUsingPrototype(hb, dagcbor.Decode, v1HeaderOrPragmaPrototype)
	if err != nil {
		return cr.Count(), fmt.Errorf("invalid header: %w", err)
	}
	header := bindnode.Unwrap(node).(*V1Header)
	*h = *header
	return cr.Count(), nil
}

// ReadFromChecked populates fields of this header from the given r. If
// maxReadBytes is non-zero, it will return ErrHeaderTooLarge if the header is
// larger than maxReadBytes. Use DefaultMaxAllowedHeaderSize to set a reasonable
// default.
func (h *V1Header) ReadFromChecked(r io.Reader, maxReadBytes uint64) (int64, error) {
	cr := internalio.NewCountingReader(r)
	hb, err := lengthPrefixedRead(cr, false, maxReadBytes)
	if err != nil {
		if err == ErrSectionTooLarge {
			err = ErrHeaderTooLarge
		}
		return cr.Count(), err
	}

	bareNode, err := ipld.Decode(hb, dagcbor.Decode)
	if err != nil {
		return cr.Count(), fmt.Errorf("invalid header: %w", err)
	}
	nb := v1HeaderOrPragmaPrototype.NewBuilder()
	if err := nb.AssignNode(bareNode); err != nil {
		return cr.Count(), fmt.Errorf("invalid header: %w", err)
	}
	node := nb.Build()
	header := bindnode.Unwrap(node).(*V1Header)
	switch header.Version {
	case 1:
		// TODO: consider dropping this entirely and allowing roots to be
		// empty. The behaviour here matches js-car, but we could change it
		// there too.
		roots, err := bareNode.LookupByString("roots")
		if err != nil || roots.Length() < 0 {
			return cr.Count(), fmt.Errorf("invalid header: no roots")
		}
	case 2:
	default:
		return cr.Count(), fmt.Errorf("invalid car version: %d", header.Version)
	}
	*h = *header
	return cr.Count(), nil
}

// ReadFrom populates fields of this header from the given r. It is an alias for
// ReadFromChecked but uses DefaultMaxAllowedHeaderSize.
func (h *V1Header) ReadFrom(r io.Reader) (int64, error) {
	return h.ReadFromChecked(r, DefaultMaxAllowedHeaderSize)
}

func (h V1Header) WriteTo(w io.Writer) (int64, error) {
	byts, err := headerBytes(h)
	if err != nil {
		return 0, err
	}
	cw := internalio.NewCountingWriter(w)
	err = LengthPrefixedWrite(cw, byts)
	return cw.Count(), err
}

func headerBytes(h V1Header) ([]byte, error) {
	node := bindnode.Wrap(&h, v1HeaderPrototype.Type())
	return ipld.Encode(node.Representation(), dagcbor.Encode)
}

func (h V1Header) WriteSize() (int64, error) {
	byts, err := headerBytes(h)
	if err != nil {
		return 0, err
	}
	return lengthPrefixedSize(byts), nil
}

func init() {
	ts, err := ipld.LoadSchemaBytes([]byte(V1HeaderSchema))
	if err != nil {
		panic(err)
	}
	schemaType := ts.TypeByName("CarV1Header")
	v1HeaderPrototype = bindnode.Prototype((*V1Header)(nil), schemaType)
	schemaType = ts.TypeByName("CarV1HeaderOrV2Pragma")
	v1HeaderOrPragmaPrototype = bindnode.Prototype((*V1Header)(nil), schemaType)
}
