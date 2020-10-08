package carbs

import "io"

type unatreader struct {
	io.ReaderAt
	at int64
}

func (u unatreader) Read(p []byte) (n int, err error) {
	n, err = u.ReadAt(p, u.at)
	u.at = u.at + int64(n)
	return
}
