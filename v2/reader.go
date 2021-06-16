package car

import (
	"bufio"
	"fmt"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"io"

	carv1 "github.com/ipld/go-car"
	"github.com/ipld/go-car/v2/carbs"
)

const (
	version2   = 2
	indexCodec = carbs.IndexSorted
)

type (
	Reader struct {
		Header Header
		r      io.ReaderAt
	}
)

func NewReader(r io.ReaderAt) (*Reader, error) {
	cr := &Reader{
		r: r,
	}
	if err := cr.readPrefix(); err != nil {
		return nil, err
	}
	if err := cr.readHeader(); err != nil {
		return nil, err
	}
	return cr, nil
}

func (r *Reader) readPrefix() (err error) {
	prefixSection := r.prefixReader()
	header, err := carv1.ReadHeader(bufio.NewReader(prefixSection))
	if err != nil {
		return
	}
	if header.Version != version2 {
		err = fmt.Errorf("invalid car version: %d", header.Version)
	}
	return
}

func (r *Reader) prefixReader() *io.SectionReader {
	return io.NewSectionReader(r.r, 0, PrefixBytesSize)
}

func (r *Reader) readHeader() (err error) {
	headerSection := r.headerReader()
	_, err = r.Header.ReadFrom(headerSection)
	return
}

func (r *Reader) headerReader() *io.SectionReader {
	return io.NewSectionReader(r.r, PrefixBytesSize, HeaderBytesSize)
}

func (r *Reader) CarV1() (*carv1.CarReader, error) {
	carV1Section := r.carV1Reader()
	return carv1.NewCarReader(bufio.NewReader(carV1Section))
}

func (r *Reader) carV1Reader() *io.SectionReader {
	return io.NewSectionReader(r.r, int64(r.Header.CarV1Offset), int64(r.Header.CarV1Size))
}

func (r *Reader) Index() (carbs.Index, error) {
	// TODO add codec to the index written into the CAR v2 so that we can infer codec automatically.
	indexCls, ok := carbs.IndexAtlas[indexCodec]
	if !ok {
		return nil, fmt.Errorf("unknown index codec: %#v", indexCodec)
	}
	index := indexCls()
	indexSection := NewOffsetReader(r.r, int64(r.Header.IndexOffset), 0)
	if err := index.Unmarshal(indexSection); err != nil {
		return nil, err
	}
	return index, nil
}

func (r *Reader) BlockStore() (blockstore.Blockstore, error) {
	index, err := r.Index()
	if err != nil {
		return nil, err
	}
	return carbs.Of(r.carV1Reader(), index), nil
}
