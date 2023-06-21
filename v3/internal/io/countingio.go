package io

import "io"

var _ io.Reader = (*CountingReader)(nil)
var _ io.ByteReader = (*CountingReader)(nil)
var _ io.Writer = (*CountingWriter)(nil)

type CountingReader struct {
	r io.Reader
	n int64
}

func NewCountingReader(r io.Reader) *CountingReader {
	return &CountingReader{r: r}
}

func (cr *CountingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.n += int64(n)
	return n, err
}

func (cr *CountingReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	_, err := cr.Read(b)
	return b[0], err
}

func (cr *CountingReader) Count() int64 {
	return cr.n
}

type CountingWriter struct {
	w io.Writer
	n int64
}

func NewCountingWriter(w io.Writer) *CountingWriter {
	return &CountingWriter{w: w}
}

func (cw *CountingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

func (cw *CountingWriter) Count() int64 {
	return cw.n
}
