package blockstore

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"golang.org/x/exp/mmap"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	carv1 "github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	"github.com/ipld/go-car/v2/internal/index"
	internalio "github.com/ipld/go-car/v2/internal/io"
)

var _ blockstore.Blockstore = (*ReadOnlyBlockStore)(nil)

// errUnsupported is returned for unsupported operations
var errUnsupported = errors.New("unsupported operation")

type (
	// ReadOnlyBlockStore provides a read-only Car Block Store.
	ReadOnlyBlockStore struct {
		backing io.ReaderAt
		idx     index.Index
	}
)

// ReadOnlyOf opens a carbs data store from an existing reader of the base data and index
func ReadOnlyOf(backing io.ReaderAt, index index.Index) *ReadOnlyBlockStore {
	return &ReadOnlyBlockStore{backing, index}
}

// LoadReadOnly opens a read-only blockstore, generating an index if it does not exist
func LoadReadOnly(path string, noPersist bool) (*ReadOnlyBlockStore, error) {
	reader, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}
	idx, err := index.Restore(path)
	if err != nil {
		idx, err = index.GenerateIndex(reader, 0, index.IndexSorted)
		if err != nil {
			return nil, err
		}
		if !noPersist {
			if err = index.Save(idx, path); err != nil {
				return nil, err
			}
		}
	}
	obj := ReadOnlyBlockStore{
		backing: reader,
		idx:     idx,
	}
	return &obj, nil
}

func (b *ReadOnlyBlockStore) read(idx int64) (cid.Cid, []byte, error) {
	bcid, data, err := util.ReadNode(bufio.NewReader(internalio.NewOffsetReader(b.backing, idx)))
	return bcid, data, err
}

// DeleteBlock is unsupported and always returns an error
func (b *ReadOnlyBlockStore) DeleteBlock(_ cid.Cid) error {
	return errUnsupported
}

// Has indicates if the store has a cid
func (b *ReadOnlyBlockStore) Has(key cid.Cid) (bool, error) {
	offset, err := b.idx.Get(key)
	if err != nil {
		return false, err
	}
	uar := internalio.NewOffsetReader(b.backing, int64(offset))
	_, err = binary.ReadUvarint(uar)
	if err != nil {
		return false, err
	}
	c, _, err := internalio.ReadCid(b.backing, uar.Offset())
	if err != nil {
		return false, err
	}
	return c.Equals(key), nil
}

// Get gets a block from the store
func (b *ReadOnlyBlockStore) Get(key cid.Cid) (blocks.Block, error) {
	offset, err := b.idx.Get(key)
	if err != nil {
		return nil, err
	}
	entry, bytes, err := b.read(int64(offset))
	if err != nil {
		// TODO Improve error handling; not all errors mean NotFound.
		return nil, blockstore.ErrNotFound
	}
	if !entry.Equals(key) {
		return nil, blockstore.ErrNotFound
	}
	return blocks.NewBlockWithCid(bytes, key)
}

// GetSize gets how big a item is
func (b *ReadOnlyBlockStore) GetSize(key cid.Cid) (int, error) {
	idx, err := b.idx.Get(key)
	if err != nil {
		return -1, err
	}
	l, err := binary.ReadUvarint(internalio.NewOffsetReader(b.backing, int64(idx)))
	if err != nil {
		return -1, blockstore.ErrNotFound
	}
	c, _, err := internalio.ReadCid(b.backing, int64(idx+l))
	if err != nil {
		return 0, err
	}
	if !c.Equals(key) {
		return -1, blockstore.ErrNotFound
	}
	// get cid. validate.
	return int(l), err
}

// Put is not supported and always returns an error
func (b *ReadOnlyBlockStore) Put(blocks.Block) error {
	return errUnsupported
}

// PutMany is not supported and always returns an error
func (b *ReadOnlyBlockStore) PutMany([]blocks.Block) error {
	return errUnsupported
}

// AllKeysChan returns the list of keys in the store
func (b *ReadOnlyBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	header, err := carv1.ReadHeader(bufio.NewReader(internalio.NewOffsetReader(b.backing, 0)))
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}
	offset, err := carv1.HeaderSize(header)
	if err != nil {
		return nil, err
	}

	ch := make(chan cid.Cid, 5)
	go func() {
		done := ctx.Done()

		rdr := internalio.NewOffsetReader(b.backing, int64(offset))
		for {
			l, err := binary.ReadUvarint(rdr)
			thisItemForNxt := rdr.Offset()
			if err != nil {
				return
			}
			c, _, err := internalio.ReadCid(b.backing, thisItemForNxt)
			if err != nil {
				return
			}
			rdr.SeekOffset(thisItemForNxt + int64(l))

			select {
			case ch <- c:
				continue
			case <-done:
				return
			}
		}
	}()
	return ch, nil
}

// HashOnRead does nothing
func (b *ReadOnlyBlockStore) HashOnRead(bool) {
}

// Roots returns the root CIDs of the backing car
func (b *ReadOnlyBlockStore) Roots() ([]cid.Cid, error) {
	header, err := carv1.ReadHeader(bufio.NewReader(internalio.NewOffsetReader(b.backing, 0)))
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}
	return header.Roots, nil
}
