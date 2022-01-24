package car

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"golang.org/x/xerrors"
)

// CarReaderSeeker wraps CarOffsetWriter with a ReadSeeker implementation.
// Note that Read and Seek are not thread-safe, they must not be called
// concurrently.
type CarReaderSeeker struct {
	parentCtx context.Context
	size      uint64
	offset    int64

	cow           *CarOffsetWriter // 🐮
	reader        *io.PipeReader
	writeCancel   context.CancelFunc
	writeComplete chan struct{}
}

var _ io.ReadSeeker = (*CarReaderSeeker)(nil)

func NewCarReaderSeeker(ctx context.Context, payloadCid cid.Cid, bstore blockstore.Blockstore, size uint64) *CarReaderSeeker {
	cow := NewCarOffsetWriter(payloadCid, bstore)

	return &CarReaderSeeker{
		parentCtx:     ctx,
		size:          size,
		cow:           cow,
		writeComplete: make(chan struct{}, 1),
	}
}

// Note: not threadsafe
func (c *CarReaderSeeker) Read(p []byte) (n int, err error) {
	if uint64(c.offset) >= c.size {
		return 0, fmt.Errorf("cannot read from offset %d >= file size %d", c.offset, c.size)
	}

	// Check if there's already a write in progress
	if c.reader == nil {
		// No write in progress, start a new write from the current offset
		// in a go routine
		writeCtx, writeCancel := context.WithCancel(c.parentCtx)
		c.writeCancel = writeCancel
		pr, pw := io.Pipe()
		c.reader = pr

		go func() {
			err := c.cow.Write(writeCtx, pw, uint64(c.offset))
			if err != nil && !xerrors.Is(err, context.Canceled) {
				pw.CloseWithError(err)
			} else {
				pw.Close()
			}
			c.writeComplete <- struct{}{}
		}()
	}

	return c.reader.Read(p)
}

// Note: not threadsafe
func (c *CarReaderSeeker) Seek(offset int64, whence int) (int64, error) {
	// Update the offset
	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return 0, fmt.Errorf("invalid offset %d from start: must be positive", offset)
		}
		c.offset = offset
	case io.SeekCurrent:
		if c.offset+offset < 0 {
			return 0, fmt.Errorf("invalid offset %d from current %d: resulting offset is negative", offset, c.offset)
		}
		c.offset += offset
	case io.SeekEnd:
		if int64(c.size)+offset < 0 {
			return 0, fmt.Errorf("invalid offset %d from end: larger than total size %d", offset, c.size)
		}
		c.offset = int64(c.size) + offset
	}

	// Cancel any ongoing write and wait for it to complete
	if c.reader != nil {
		c.writeCancel()

		// Seek and Read should not be called concurrently so this is safe
		c.reader.Close()

		select {
		case <-c.parentCtx.Done():
			return 0, c.parentCtx.Err()
		case <-c.writeComplete:
		}

		c.reader = nil
	}

	return c.offset, nil
}
