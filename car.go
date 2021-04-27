package car

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	posinfo "github.com/ipfs/go-ipfs-posinfo"
	cbor "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-log/v2"
	dag "github.com/ipfs/go-merkledag"

	util "github.com/ipld/go-car/util"
)

var logger = log.Logger("go-car")

func init() {
	cbor.RegisterCborType(CarHeader{})
}

type Store interface {
	Put(blocks.Block) error
}

type ReadStore interface {
	Get(cid.Cid) (blocks.Block, error)
}

type CarHeader struct {
	Roots   []cid.Cid
	Version uint64
}

type carWriter struct {
	ds   format.NodeGetter
	w    io.Writer
	walk WalkFunc
}

type WalkFunc func(format.Node) ([]*format.Link, error)

func WriteCar(ctx context.Context, ds format.NodeGetter, roots []cid.Cid, w io.Writer) error {
	return WriteCarWithWalker(ctx, ds, roots, w, DefaultWalkFunc)
}

func WriteCarWithWalker(ctx context.Context, ds format.NodeGetter, roots []cid.Cid, w io.Writer, walk WalkFunc) error {

	h := &CarHeader{
		Roots:   roots,
		Version: 1,
	}

	if err := WriteHeader(h, w); err != nil {
		return fmt.Errorf("failed to write car header: %s", err)
	}

	cw := &carWriter{ds: ds, w: w, walk: walk}
	seen := cid.NewSet()
	for _, r := range roots {
		if err := dag.Walk(ctx, cw.enumGetLinks, r, seen.Visit); err != nil {
			return err
		}
	}
	return nil
}

func DefaultWalkFunc(nd format.Node) ([]*format.Link, error) {
	return nd.Links(), nil
}

func ReadHeader(br *bufio.Reader) (*CarHeader, uint64, error) {
	hb, l, err := util.LdRead(br)
	if err != nil {
		return nil, 0, err
	}

	var ch CarHeader
	if err := cbor.DecodeInto(hb, &ch); err != nil {
		return nil, 0, err
	}

	return &ch, l, nil
}

func WriteHeader(h *CarHeader, w io.Writer) error {
	hb, err := cbor.DumpObject(h)
	if err != nil {
		return err
	}

	return util.LdWrite(w, hb)
}

func HeaderSize(h *CarHeader) (uint64, error) {
	hb, err := cbor.DumpObject(h)
	if err != nil {
		return 0, err
	}

	return util.LdSize(hb), nil
}

func (cw *carWriter) enumGetLinks(ctx context.Context, c cid.Cid) ([]*format.Link, error) {
	nd, err := cw.ds.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	if err := cw.writeNode(ctx, nd); err != nil {
		return nil, err
	}

	return cw.walk(nd)
}

func (cw *carWriter) writeNode(ctx context.Context, nd format.Node) error {
	return util.LdWrite(cw.w, nd.Cid().Bytes(), nd.RawData())
}

type CarReader struct {
	br       *bufio.Reader
	offset   uint64
	fullPath string
	stat     os.FileInfo
	Header   *CarHeader
}

func NewCarReader(r io.Reader) (*CarReader, error) {
	br := bufio.NewReader(r)
	ch, offset, err := ReadHeader(br)
	if err != nil {
		return nil, err
	}

	if len(ch.Roots) == 0 {
		return nil, fmt.Errorf("empty car")
	}

	if ch.Version != 1 {
		return nil, fmt.Errorf("invalid car version: %d", ch.Version)
	}

	cr := &CarReader{
		br:     br,
		offset: offset,
		Header: ch,
	}
	if fi, ok := r.(files.FileInfo); ok {
		cr.fullPath = fi.AbsPath()
		cr.stat = fi.Stat()
	}
	return cr, nil
}

func (cr *CarReader) Next() (blocks.Block, error) {
	c, l, data, err := util.ReadNode(cr.br)
	if err != nil {
		return nil, err
	}

	hashed, err := c.Prefix().Sum(data)
	if err != nil {
		return nil, err
	}

	if !hashed.Equals(c) {
		return nil, fmt.Errorf("mismatch in content integrity, name: %s, data: %s", c, hashed)
	}

	offset := cr.offset + l - uint64(len(data))
	cr.offset += l

	blk, err := blocks.NewBlockWithCid(data, c)
	if cr.fullPath == "" {
		return blk, err
	}
	nd, err := format.Decode(blk)
	if err != nil {
		return nil, err
	}
	return &posinfo.FilestoreNode{
		Node: nd,
		PosInfo: &posinfo.PosInfo{
			Offset:   offset,
			FullPath: cr.fullPath,
			Stat:     cr.stat,
		},
	}, nil
}

type batchStore interface {
	PutMany([]blocks.Block) error
}

func LoadCar(s Store, r io.Reader) (*CarHeader, error) {
	logger.Debug("will load car now")
	cr, err := NewCarReader(r)
	if err != nil {
		return nil, err
	}

	if bs, ok := s.(batchStore); ok {
		logger.Debug("will load car fast")
		return loadCarFast(bs, cr)
	}

	logger.Debug("will load car slow")
	return loadCarSlow(s, cr)
}

func loadCarFast(s batchStore, cr *CarReader) (*CarHeader, error) {
	nBlocks := 0
	var buf []blocks.Block
	for {
		blk, err := cr.Next()
		switch err {
		case io.EOF:
			if len(buf) > 0 {
				if err := s.PutMany(buf); err != nil {
					logger.Errorf("failed to write %d blocks on completion, nBlocksWritten=%d,  err=%s", len(buf), nBlocks, err)
					return nil, err
				}
				nBlocks = nBlocks + len(buf)
			}

			logger.Debugf("successfully loaded CAR, nBlocksWritten=%d", nBlocks)
			return cr.Header, nil
		default:
			logger.Errorf("failed to load car, nBlocksWritten=%d, err=%s", nBlocks, err)
			return nil, err
		case nil:
		}

		buf = append(buf, blk)
		logger.Debugf("queued for writing to blockstore, cid=%s, nBlocksWritten=%d, current buffer size=%d", blk.Cid(), nBlocks, len(buf))

		if len(buf) > 1000 {
			if err := s.PutMany(buf); err != nil {
				logger.Errorf("failed to write %d blocks, nBlocksWritten=%d, err=%s", len(buf), nBlocks, err)
				return nil, err
			}
			nBlocks = nBlocks + len(buf)
			buf = buf[:0]
		}
	}
}

func loadCarSlow(s Store, cr *CarReader) (*CarHeader, error) {
	nBlocks := 0
	for {
		blk, err := cr.Next()
		switch err {
		case io.EOF:
			logger.Debugf("loaded CAR successfully, nBlocksWritten=%d", nBlocks)
			return cr.Header, nil
		default:
			logger.Errorf("failed to load CAR, err=%s, nBlocksWritten=%d", err, nBlocks)
			return nil, err
		case nil:
		}

		if err := s.Put(blk); err != nil {
			logger.Errorf("failed to write block with cid=%s to the blockstore, err=%s, nBlocksWritten=%d", blk.Cid(), err, nBlocks)
			return nil, err
		}

		nBlocks++
		logger.Debugf("successfully wrote block with cid=%s to the blockstore", blk.Cid())
	}
}
