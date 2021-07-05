package index

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/internal/carv1"
)

type readSeekerPlusByte struct {
	io.ReadSeeker
}

func (r readSeekerPlusByte) ReadByte() (byte, error) {
	var p [1]byte
	_, err := io.ReadFull(r, p[:])
	return p[0], err
}

// Generate generates index for a given car in v1 format.
// The index can be stored using index.Save into a file or serialized using index.WriteTo.
func Generate(v1 io.ReadSeeker) (Index, error) {
	header, err := carv1.ReadHeader(bufio.NewReader(v1))
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}

	// TODO: Generate should likely just take an io.ReadSeeker.
	// TODO: ensure the input's header version is 1.

	offset, err := carv1.HeaderSize(header)
	if err != nil {
		return nil, err
	}

	idx := mkSorted()

	records := make([]Record, 0)
	if _, err := v1.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}
	for {
		// Grab the length of the frame.
		// Note that ReadUvarint wants a ByteReader.
		length, err := binary.ReadUvarint(readSeekerPlusByte{v1})
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Grab the offset of the frame, where we are right now.
		frameOffset, err := v1.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		// Grab the CID.
		_, c, err := cid.CidFromReader(v1)
		if err != nil {
			return nil, err
		}
		records = append(records, Record{c, uint64(frameOffset)})

		// Seek to the next length+frame.
		if _, err := v1.Seek(frameOffset+int64(length), io.SeekStart); err != nil {
			return nil, err
		}
	}

	if err := idx.Load(records); err != nil {
		return nil, err
	}

	return idx, nil
}

// GenerateFromFile walks a car v1 file at the give path and generates an index of cid->byte offset.
// The index can be stored using index.Save into a file or serialized using index.WriteTo.
func GenerateFromFile(path string) (Index, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Generate(f)
}
