package car

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"testing"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-merkledag"
	"github.com/stretchr/testify/require"
)

func TestCarReaderSeeker(t *testing.T) {
	ctx := context.Background()

	ds := dss.MutexWrap(datastore.NewMapDatastore())
	bs := bstore.NewBlockstore(ds)
	bserv := blockservice.New(bs, nil)
	dserv := merkledag.NewDAGService(bserv)

	rseed := 5
	size := 2 * 1024 * 1024
	source := io.LimitReader(rand.New(rand.NewSource(int64(rseed))), int64(size))
	nd, err := DAGImport(dserv, source)
	require.NoError(t, err)

	// Write the CAR to a buffer so it can be used for comparisons
	fullCarCow := NewCarOffsetWriter(nd.Cid(), bs)
	var fullBuff bytes.Buffer
	err = fullCarCow.Write(context.Background(), &fullBuff, 0)
	require.NoError(t, err)
	carSize := fullBuff.Len()

	readTestCases := []struct {
		name   string
		offset int64
	}{{
		name:   "read all from start",
		offset: 0,
	}, {
		name:   "read all from byte 1",
		offset: 1,
	}, {
		name:   "read all from middle",
		offset: int64(carSize / 2),
	}, {
		name:   "read all from end - 1",
		offset: int64(carSize - 1),
	}}

	for _, tc := range readTestCases {
		t.Run(tc.name, func(t *testing.T) {
			crs := NewCarReaderSeeker(ctx, nd.Cid(), bs, uint64(carSize))
			if tc.offset > 0 {
				_, err := crs.Seek(tc.offset, io.SeekStart)
				require.NoError(t, err)
			}
			buff, err := io.ReadAll(crs)
			require.NoError(t, err)
			require.Equal(t, carSize-int(tc.offset), len(buff))
			require.Equal(t, fullBuff.Bytes()[tc.offset:], buff)
		})
	}

	seekTestCases := []struct {
		name          string
		whence        int
		offset        int64
		expectSeekErr bool
		expectReadErr bool
		expectOffset  int64
	}{{
		name:         "start +0",
		whence:       io.SeekStart,
		offset:       0,
		expectOffset: 0,
	}, {
		name:          "start -1",
		whence:        io.SeekStart,
		offset:        -1,
		expectSeekErr: true,
	}, {
		name:          "start +full size",
		whence:        io.SeekStart,
		offset:        int64(carSize),
		expectOffset:  int64(carSize),
		expectReadErr: true,
	}, {
		name:         "current +0",
		whence:       io.SeekCurrent,
		offset:       0,
		expectOffset: 0,
	}, {
		name:         "current +10",
		whence:       io.SeekCurrent,
		offset:       10,
		expectOffset: 10,
	}, {
		name:          "current -1",
		whence:        io.SeekCurrent,
		offset:        -1,
		expectSeekErr: true,
	}, {
		name:          "current +full size",
		whence:        io.SeekCurrent,
		offset:        int64(carSize),
		expectOffset:  int64(carSize),
		expectReadErr: true,
	}, {
		name:          "end +0",
		whence:        io.SeekEnd,
		offset:        0,
		expectOffset:  int64(carSize),
		expectReadErr: true,
	}, {
		name:         "end -10",
		whence:       io.SeekEnd,
		offset:       -10,
		expectOffset: int64(carSize) - 10,
	}, {
		name:          "end +1",
		whence:        io.SeekEnd,
		offset:        1,
		expectOffset:  int64(carSize) + 1,
		expectReadErr: true,
	}, {
		name:          "end -(full size+1)",
		whence:        io.SeekEnd,
		offset:        int64(-(carSize + 1)),
		expectSeekErr: true,
	}}

	for _, tc := range seekTestCases {
		t.Run(tc.name, func(t *testing.T) {
			crs := NewCarReaderSeeker(ctx, nd.Cid(), bs, uint64(carSize))
			newOffset, err := crs.Seek(tc.offset, tc.whence)
			if tc.expectSeekErr {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectOffset, newOffset)

			buff, err := io.ReadAll(crs)
			if tc.expectReadErr {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, carSize-int(tc.expectOffset), len(buff))
			require.Equal(t, fullBuff.Bytes()[tc.expectOffset:], buff)
		})
	}

	t.Run("double seek", func(t *testing.T) {
		crs := NewCarReaderSeeker(ctx, nd.Cid(), bs, uint64(carSize))
		_, err := crs.Seek(10, io.SeekStart)
		require.NoError(t, err)
		newOffset, err := crs.Seek(-5, io.SeekCurrent)
		require.NoError(t, err)
		require.EqualValues(t, 5, newOffset)

		buff, err := io.ReadAll(crs)
		require.NoError(t, err)

		require.Equal(t, carSize-5, len(buff))
		require.Equal(t, fullBuff.Bytes()[5:], buff)
	})
}
