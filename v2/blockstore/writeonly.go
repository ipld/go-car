package blockstore

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	blocks "github.com/ipfs/go-libipfs/blocks"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/ipld/go-car/v2/internal/carv1/util"
)

var ErrWriteOnly = errors.New("write-only blockstore; unsupported operation")

var _ blockstore.Blockstore = (*WriteOnly)(nil)

// WriteOnly is a blockstore that only supports write operations, Put and
// PutMany as well as some indexed operations: Has, GetSize and AllKeysChan.
type WriteOnly struct {
	mu     sync.RWMutex
	opts   carv2.Options
	out    io.Writer
	blocks map[string]wroteBlock
}

type wroteBlock struct {
	cid  cid.Cid
	size int
}

// CreateWriteOnlyV1 creates a new WriteOnly blockstore that writes to the given
// io.Writer in CARv1 format. A WriteOnly CARv1 is able to support most of the
// Blockstore interface while also being suitable for writing to a streaming
// output as it only requires an io.Writer.
//
// The returned WriteOnly blockstore will not support Get or DeleteBlock
// operations, but keeps track of written CIDs so is able to support Has,
// GetSize and AllKeysChan operations.
func CreateWriteOnlyV1(out io.Writer, roots []cid.Cid, opts ...carv2.Option) (*WriteOnly, error) {
	wo := &WriteOnly{
		out:    out,
		opts:   carv2.ApplyOptions(opts...),
		blocks: make(map[string]wroteBlock),
	}
	if err := carv1.WriteHeader(&carv1.CarHeader{Roots: roots, Version: 1}, out); err != nil {
		return nil, err
	}
	return wo, nil
}

// DeleteBlock is not supported by WriteOnly blockstore.
func (wo *WriteOnly) DeleteBlock(context.Context, cid.Cid) error {
	return ErrWriteOnly
}

// Has returns true if the blockstore contains the given CID. The
// StoreIdentityCIDs option controls whether IDENTITY CIDs are considered to be
// present in the blockstore.
func (wo *WriteOnly) Has(ctx context.Context, c cid.Cid) (bool, error) {
	if !wo.opts.StoreIdentityCIDs {
		// If we don't store identity CIDs then we can return them straight away as if they are here,
		// otherwise we need to check for their existence.
		// Note, we do this without locking, since there is no shared information to lock for in order to perform the check.
		if _, ok, err := isIdentity(c); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}

	wo.mu.RLock()
	defer wo.mu.RUnlock()
	_, has := wo.blocks[string(c.Hash())]
	return has, nil
}

// hasExact is not locked, caller must hold lock
func (wo *WriteOnly) hasExact(c cid.Cid) bool {
	if blk, has := wo.blocks[string(c.Hash())]; !has {
		return blk.cid.Equals(c)
	}
	return false
}

// Get is not supported by WriteOnly blockstore.
func (wo *WriteOnly) Get(context.Context, cid.Cid) (blocks.Block, error) {
	return nil, ErrWriteOnly
}

// GetSize returns the size of the block with the given CID. The
// StoreIdentityCIDs option controls whether IDENTITY CIDs are considered to be
// present in the blockstore and therefore have a size.
func (wo *WriteOnly) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	if !wo.opts.StoreIdentityCIDs {
		if digest, ok, err := isIdentity(c); err != nil {
			return 0, err
		} else if ok {
			return len(digest), nil
		}
	}

	wo.mu.RLock()
	defer wo.mu.RUnlock()
	if blk, has := wo.blocks[string(c.Hash())]; has {
		return blk.size, nil
	}
	return 0, format.ErrNotFound{Cid: c}
}

// Put writes the given block to the underlying io.Writer in CARv1 format.
func (wo *WriteOnly) Put(ctx context.Context, blk blocks.Block) error {
	return wo.PutMany(ctx, []blocks.Block{blk})
}

// PutMany writes the given blocks to the underlying io.Writer in CARv1 format.
// If the StoreIdentityCIDs option is disabled then IDENTITY CIDs are ignored.
func (wo *WriteOnly) PutMany(ctx context.Context, blks []blocks.Block) error {
	wo.mu.Lock()
	defer wo.mu.Unlock()
	for _, bl := range blks {
		c := bl.Cid()

		// If StoreIdentityCIDs option is disabled then treat IDENTITY CIDs like IdStore.
		if !wo.opts.StoreIdentityCIDs {
			// Check for IDENTITY CID. If IDENTITY, ignore and move to the next block.
			if _, ok, err := isIdentity(c); err != nil {
				return err
			} else if ok {
				continue
			}
		}

		// Check if its size is too big.
		// If larger than maximum allowed size, return error.
		// Note, we need to check this regardless of whether we have IDENTITY CID or not.
		// Since multhihash codes other than IDENTITY can result in large digests.
		cSize := uint64(len(c.Bytes()))
		if cSize > wo.opts.MaxIndexCidSize {
			return &carv2.ErrCidTooLarge{MaxSize: wo.opts.MaxIndexCidSize, CurrentSize: cSize}
		}

		if !wo.opts.BlockstoreAllowDuplicatePuts {
			if wo.opts.BlockstoreUseWholeCIDs && wo.hasExact(c) {
				continue // deduplicated by CID
			}
			if !wo.opts.BlockstoreUseWholeCIDs {
				if _, has := wo.blocks[string(c.Hash())]; has {
					continue // deduplicated by hash
				}
			}
		}

		if err := util.LdWrite(wo.out, c.Bytes(), bl.RawData()); err != nil {
			return err
		}
		wo.blocks[string(c.Hash())] = wroteBlock{cid: c, size: len(bl.RawData())}
	}
	return nil
}

// AllKeysChan returns a channel from which all the CIDs in the blockstore can
// be read.
func (wo *WriteOnly) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid)
	go func() {
		wo.mu.RLock()
		defer wo.mu.RUnlock()
		defer close(ch)
		for _, blk := range wo.blocks {
			select {
			case <-ctx.Done():
				return
			default:
			}
			ch <- blk.cid
		}
	}()
	return ch, nil
}

// HashOnRead is not supported by WriteOnly blockstore.
func (wo *WriteOnly) HashOnRead(_ bool) {}
