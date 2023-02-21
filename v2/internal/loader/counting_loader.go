package loader

import (
	"bytes"
	"io"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/multiformats/go-varint"
)

// Counter tracks how much data has been read.
type Counter struct {
	TotalRead uint64
}

func (c *Counter) Size() uint64 {
	return c.TotalRead
}

// ReadCounter provides an externally consumable interface to the
// additional data tracked about the linksystem.
type ReadCounter interface {
	Size() uint64
}

type countingReader struct {
	r io.Reader
	c *Counter
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.c.TotalRead += uint64(n)
	return n, err
}

// CountingLinkSystem wraps an ipld linksystem with to track the size of
// data loaded in a `counter` object. Each time nodes are loaded from the
// link system which trigger block reads, the size of the block as it would
// appear in a CAR file is added to the counter (included the size of the
// CID and the varint length for the block data).
func CountingLinkSystem(ls ipld.LinkSystem) (ipld.LinkSystem, ReadCounter) {
	c := Counter{}
	clc := ls
	clc.StorageReadOpener = func(lc linking.LinkContext, l ipld.Link) (io.Reader, error) {
		r, err := ls.StorageReadOpener(lc, l)
		if err != nil {
			return nil, err
		}
		buf := bytes.NewBuffer(nil)
		n, err := buf.ReadFrom(r)
		if err != nil {
			return nil, err
		}
		size := varint.ToUvarint(uint64(n) + uint64(len(l.Binary())))
		c.TotalRead += uint64(len(size)) + uint64(len(l.Binary()))
		return &countingReader{buf, &c}, nil
	}
	return clc, &c
}
