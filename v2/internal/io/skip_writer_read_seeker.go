package io

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// SkipWriterReaderSeeker wraps a factory producing a writer with a ReadSeeker implementation.
// Note that Read and Seek are not thread-safe, they must not be called
// concurrently.
type SkipWriterReaderSeeker struct {
	parentCtx context.Context
	offset    uint64
	size      uint64

	cons          ReWriter
	reader        *io.PipeReader
	writeCancel   context.CancelFunc
	writeComplete chan struct{}
}

// ReWriter is a function writing to an io.Writer from an offset.
type ReWriter func(ctx context.Context, skip uint64, w io.Writer) (uint64, error)

var _ io.ReadSeeker = (*SkipWriterReaderSeeker)(nil)
var _ io.Closer = (*SkipWriterReaderSeeker)(nil)

// NewSkipWriterReaderSeeker creates an io.ReadSeeker around a ReWriter.
func NewSkipWriterReaderSeeker(ctx context.Context, size uint64, cons ReWriter) *SkipWriterReaderSeeker {
	return &SkipWriterReaderSeeker{
		parentCtx:     ctx,
		size:          size,
		cons:          cons,
		writeComplete: make(chan struct{}, 1),
	}
}

// Note: not threadsafe
func (c *SkipWriterReaderSeeker) Read(p []byte) (int, error) {
	// Check if there's already a write in progress
	if c.reader == nil {
		// No write in progress, start a new write from the current offset
		// in a go routine and feed it back to the caller via a pipe
		writeCtx, writeCancel := context.WithCancel(c.parentCtx)
		c.writeCancel = writeCancel
		pr, pw := io.Pipe()
		c.reader = pr

		go func() {
			amnt, err := c.cons(writeCtx, c.offset, pw)
			c.offset += amnt
			if err != nil && !errors.Is(err, context.Canceled) {
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
func (c *SkipWriterReaderSeeker) Seek(offset int64, whence int) (int64, error) {
	// Update the offset
	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return 0, fmt.Errorf("invalid offset %d from start: must be positive", offset)
		}
		c.offset = uint64(offset)
	case io.SeekCurrent:
		if int64(c.offset)+offset < 0 {
			return 0, fmt.Errorf("invalid offset %d from current %d: resulting offset is negative", offset, c.offset)
		}
		c.offset = uint64((int64(c.offset) + offset))
	case io.SeekEnd:
		if c.size == 0 {
			return 0, ErrUnsupported

		}
		if int64(c.size)+offset < 0 {
			return 0, fmt.Errorf("invalid offset %d from end: larger than total size %d", offset, c.size)
		}
		c.offset = uint64(int64(c.size) + offset)
	}

	// Cancel any ongoing write and wait for it to complete
	// TODO: if we're fast-forwarding with 'SeekCurrent', we may be able to read from the current reader instead.
	if c.reader != nil {
		err := c.Close()
		c.reader = nil
		if err != nil {
			return 0, err
		}
	}

	return int64(c.offset), nil
}

func (c *SkipWriterReaderSeeker) Close() error {
	c.writeCancel()
	// Seek and Read should not be called concurrently so this is safe
	c.reader.Close()

	select {
	case <-c.parentCtx.Done():
		return c.parentCtx.Err()
	case <-c.writeComplete:
	}
	return nil
}
