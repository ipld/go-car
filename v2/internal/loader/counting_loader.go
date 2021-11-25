package loader

import (
	"io"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/multiformats/go-varint"
)

// counter tracks how much data has been read.
type counter struct {
	totalRead uint64
}

func (c *counter) Size() uint64 {
	return c.totalRead
}

// ReadCounter provides an externally consumable interface to the
// additional data tracked about the linksystem.
type ReadCounter interface {
	Size() uint64
}

type countingReader struct {
	r    io.Reader
	c    *counter
	read uint64
	cid  string
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if err == io.EOF {
		// add in the overall length of the block.
		n += len(c.cid)
		uv := varint.ToUvarint(uint64(n))
		n += len(uv)
	}
	c.c.totalRead += uint64(n)
	c.read += uint64(n)
	return n, err
}

// CountingLinkSystem wraps an ipld linksystem with to track the size of
// data loaded in a `counter` object. Each time nodes are loaded from the
// link system which trigger block reads, the size of the block as it would
// appear in a CAR file is added to the counter (included the size of the
// CID and the varint length for the block data).
func CountingLinkSystem(ls ipld.LinkSystem) (ipld.LinkSystem, ReadCounter) {
	c := counter{}
	return linking.LinkSystem{
		EncoderChooser:     ls.EncoderChooser,
		DecoderChooser:     ls.DecoderChooser,
		HasherChooser:      ls.HasherChooser,
		StorageWriteOpener: ls.StorageWriteOpener,
		StorageReadOpener: func(lc linking.LinkContext, l ipld.Link) (io.Reader, error) {
			r, err := ls.StorageReadOpener(lc, l)
			if err != nil {
				return nil, err
			}
			return &countingReader{r, &c, 0, l.Binary()}, nil
		},
		TrustedStorage: ls.TrustedStorage,
		NodeReifier:    ls.NodeReifier,
	}, &c
}
