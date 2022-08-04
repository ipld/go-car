package loader

import (
	"bytes"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

// indexingWriter wraps an io.Writer with metadata of the index of the car written to it.
type indexingWriter struct {
	w      io.Writer
	size   uint64
	toSkip uint64
	code   multicodec.Code
	rcrds  map[cid.Cid]index.Record
}

func (w *indexingWriter) Size() uint64 {
	return w.size
}

func (w *indexingWriter) Index() (index.Index, error) {
	idx, err := index.New(w.code)
	if err != nil {
		return nil, err
	}
	// todo: maybe keep both a map and a list proactively for efficiency here.
	rcrds := make([]index.Record, 0, len(w.rcrds))
	for _, r := range w.rcrds {
		rcrds = append(rcrds, r)
	}
	if err := idx.Load(rcrds); err != nil {
		return nil, err
	}

	return idx, nil
}

// An IndexTracker tracks the records loaded/written, calculate an
// index based on them.
type IndexTracker interface {
	ReadCounter
	Index() (index.Index, error)
}

var _ IndexTracker = (*indexingWriter)(nil)

type writingReader struct {
	r   io.Reader
	buf []byte
	cid string
	wo  *indexingWriter
}

func (w *writingReader) Read(p []byte) (int, error) {
	if w.wo != nil {
		// build the buffer of size:cid:block if we don't have it yet.
		buf := bytes.NewBuffer(nil)
		// allocate space for size
		_, err := buf.Write(make([]byte, varint.MaxLenUvarint63))
		if err != nil {
			return 0, err
		}
		// write the cid
		if _, err := buf.Write([]byte(w.cid)); err != nil {
			return 0, err
		}
		// write the block
		n, err := io.Copy(buf, w.r)
		if err != nil {
			return 0, err
		}
		sizeBytes := varint.ToUvarint(uint64(n) + uint64(len(w.cid)))
		writeBuf := buf.Bytes()[varint.MaxLenUvarint63-len(sizeBytes):]
		w.buf = buf.Bytes()[varint.MaxLenUvarint63+len(w.cid):]
		_ = copy(writeBuf[:], sizeBytes)

		size := len(writeBuf)
		if w.wo.toSkip > 0 {
			if w.wo.toSkip >= uint64(len(writeBuf)) {
				w.wo.toSkip -= uint64(len(writeBuf))
				writeBuf = []byte{}
			} else {
				writeBuf = writeBuf[w.wo.toSkip:]
				w.wo.toSkip = 0
			}
		}

		if _, err := bytes.NewBuffer(writeBuf).WriteTo(w.wo.w); err != nil {
			return 0, err
		}
		_, c, err := cid.CidFromBytes([]byte(w.cid))
		if err != nil {
			return 0, err
		}
		w.wo.rcrds[c] = index.Record{
			Cid:    c,
			Offset: w.wo.size,
		}
		w.wo.size += uint64(size)
		w.wo = nil
	}

	if w.buf != nil {
		n, err := bytes.NewBuffer(w.buf).Read(p)
		if err != nil {
			return n, err
		}
		w.buf = w.buf[n:]
		return n, err
	}

	return w.r.Read(p)
}

// TeeingLinkSystem wraps an IPLD.LinkSystem so that each time a block is loaded from it,
// that block is also written as a CAR block to the provided io.Writer. Metadata
// (the size of data written) is provided in the second return value.
// The `initialOffset` is used to calculate the offsets recorded for the index, and will be
//   included in the `.Size()` of the IndexTracker.
// An indexCodec of `index.CarIndexNoIndex` can be used to not track these offsets.
func TeeingLinkSystem(ls ipld.LinkSystem, w io.Writer, initialOffset uint64, skip uint64, indexCodec multicodec.Code) (ipld.LinkSystem, IndexTracker) {
	iw := indexingWriter{
		w:      w,
		size:   initialOffset,
		toSkip: skip,
		code:   indexCodec,
		rcrds:  make(map[cid.Cid]index.Record),
	}

	tls := ls
	tls.StorageReadOpener = func(lc linking.LinkContext, l ipld.Link) (io.Reader, error) {
		_, c, err := cid.CidFromBytes([]byte(l.Binary()))
		if err != nil {
			return nil, err
		}

		// if we've already read this cid in this session, don't re-write it.
		if _, ok := iw.rcrds[c]; ok {
			return ls.StorageReadOpener(lc, l)
		}

		r, err := ls.StorageReadOpener(lc, l)
		if err != nil {
			return nil, err
		}

		return &writingReader{r, nil, l.Binary(), &iw}, nil
	}
	return tls, &iw
}
