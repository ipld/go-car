package testutil

import (
	"io"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	car "github.com/ipld/go-car/v3"
	"github.com/stretchr/testify/require"
)

func NewV1ReaderFromV1File(t *testing.T, carv1Path string, zeroLenSectionAsEOF bool) car.BlockReader {
	f, err := os.Open(carv1Path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	v1r, err := NewV1Reader(f, zeroLenSectionAsEOF, 0, 0)
	require.NoError(t, err)
	return v1r
}

func NewV1ReaderFromV2File(t *testing.T, carv2Path string, zeroLenSectionAsEOF bool) car.BlockReader {
	f, err := os.Open(carv2Path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	v2r, err := car.NewV2Reader(f)
	require.NoError(t, err)
	dr, err := v2r.DataReader()
	require.NoError(t, err)
	v1r, err := NewV1Reader(dr, zeroLenSectionAsEOF, 0, 0)
	require.NoError(t, err)
	return v1r
}

func ListCids(t *testing.T, v1r car.BlockReader) (cids []cid.Cid) {
	for {
		c, _, err := v1r.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		cids = append(cids, c)
	}
	return
}
