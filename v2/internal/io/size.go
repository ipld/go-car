package io

import "io"

// Size returns the total number of readable bytes in r, discarding any bytes read.
func Size(r io.Reader) (uint64, error) {
	buf := make([]byte, 1024)
	var size uint64
	for {
		read, err := r.Read(buf)
		size += uint64(read)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return size, err
		}
	}
}
