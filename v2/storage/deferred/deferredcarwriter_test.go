package deferred_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	deferred "github.com/ipld/go-car/v2/storage/deferred"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var rng = rand.New(rand.NewSource(3333))
var rngLk sync.Mutex

func TestDeferredCarWriterForPath(t *testing.T) {
	req := require.New(t)

	ctx := context.Background()
	testCid1, testData1 := randBlock()
	testCid2, testData2 := randBlock()

	for version := 1; version <= 2; version++ {
		t.Run(fmt.Sprintf("version=%d", version), func(t *testing.T) {
			tmpFile := t.TempDir() + "/test.car"

			opts := []carv2.Option{}
			if version == 1 {
				opts = append(opts, carv2.WriteAsCarV1(true))
			}
			cw := deferred.NewDeferredCarWriterForPath(tmpFile, []cid.Cid{testCid1}, opts...)

			_, err := os.Stat(tmpFile)
			req.True(os.IsNotExist(err))

			req.NoError(cw.Put(ctx, testCid1.KeyString(), testData1))
			req.NoError(cw.Put(ctx, testCid2.KeyString(), testData2))

			stat, err := os.Stat(tmpFile)
			req.NoError(err)
			req.True(stat.Size() > int64(len(testData1)+len(testData2)))

			req.NoError(cw.Close())

			// shouldn't be deleted
			_, err = os.Stat(tmpFile)
			req.NoError(err)

			r, err := os.Open(tmpFile)
			req.NoError(err)
			t.Cleanup(func() { r.Close() })
			carv2, err := carv2.NewBlockReader(r)
			req.NoError(err)

			// compare CAR contents to what we wrote
			req.Equal([]cid.Cid{testCid1}, carv2.Roots)
			req.Equal(uint64(version), carv2.Version)

			blk, err := carv2.Next()
			req.NoError(err)
			req.Equal(testCid1, blk.Cid())
			req.Equal(testData1, blk.RawData())

			blk, err = carv2.Next()
			req.NoError(err)
			req.Equal(testCid2, blk.Cid())
			req.Equal(testData2, blk.RawData())

			_, err = carv2.Next()
			req.ErrorIs(io.EOF, err)
		})
	}
}

func TestDeferredCarWriter(t *testing.T) {
	for _, tc := range []string{"path", "stream"} {
		tc := tc
		t.Run(tc, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			testCid1, testData1 := randBlock()
			testCid2, testData2 := randBlock()
			testCid3, _ := randBlock()

			var cw *deferred.DeferredCarWriter
			var buf bytes.Buffer
			tmpFile := t.TempDir() + "/test.car"

			if tc == "path" {
				cw = deferred.NewDeferredCarWriterForPath(tmpFile, []cid.Cid{testCid1}, carv2.WriteAsCarV1(true))
				_, err := os.Stat(tmpFile)
				require.True(t, os.IsNotExist(err))
			} else {
				cw = deferred.NewDeferredCarWriterForStream(&buf, []cid.Cid{testCid1})
				require.Equal(t, buf.Len(), 0)
			}

			has, err := cw.Has(ctx, testCid3.KeyString())
			require.NoError(t, err)
			require.False(t, has)

			require.NoError(t, cw.Put(ctx, testCid1.KeyString(), testData1))
			has, err = cw.Has(ctx, testCid1.KeyString())
			require.NoError(t, err)
			require.True(t, has)
			require.NoError(t, cw.Put(ctx, testCid2.KeyString(), testData2))
			has, err = cw.Has(ctx, testCid1.KeyString())
			require.NoError(t, err)
			require.True(t, has)
			has, err = cw.Has(ctx, testCid2.KeyString())
			require.NoError(t, err)
			require.True(t, has)
			has, err = cw.Has(ctx, testCid3.KeyString())
			require.NoError(t, err)
			require.False(t, has)

			if tc == "path" {
				stat, err := os.Stat(tmpFile)
				require.NoError(t, err)
				require.True(t, stat.Size() > int64(len(testData1)+len(testData2)))
			} else {
				require.True(t, buf.Len() > len(testData1)+len(testData2))
			}

			require.NoError(t, cw.Close())

			var rdr *carv2.BlockReader
			if tc == "path" {
				r, err := os.Open(tmpFile)
				require.NoError(t, err)
				rdr, err = carv2.NewBlockReader(r)
				require.NoError(t, err)
				t.Cleanup(func() { r.Close() })
			} else {
				rdr, err = carv2.NewBlockReader(&buf)
				require.NoError(t, err)
			}

			// compare CAR contents to what we wrote
			require.Equal(t, rdr.Roots, []cid.Cid{testCid1})
			require.Equal(t, rdr.Version, uint64(1))

			blk, err := rdr.Next()
			require.NoError(t, err)
			require.Equal(t, blk.Cid(), testCid1)
			require.Equal(t, blk.RawData(), testData1)

			blk, err = rdr.Next()
			require.NoError(t, err)
			require.Equal(t, blk.Cid(), testCid2)
			require.Equal(t, blk.RawData(), testData2)

			_, err = rdr.Next()
			require.ErrorIs(t, err, io.EOF)
		})
	}
}

func TestDeferredCarWriterPutCb(t *testing.T) {
	ctx := context.Background()
	testCid1, testData1 := randBlock()
	testCid2, testData2 := randBlock()

	var buf bytes.Buffer
	cw := deferred.NewDeferredCarWriterForStream(&buf, []cid.Cid{testCid1})

	var pc1 int
	cw.OnPut(func(ii int) {
		switch pc1 {
		case 0:
			require.Equal(t, buf.Len(), 0) // called before first write
			require.Equal(t, len(testData1), ii)
		case 1:
			require.Equal(t, len(testData2), ii)
		default:
			require.Fail(t, "unexpected put callback")
		}
		pc1++
	}, false)
	var pc2 int
	cw.OnPut(func(ii int) {
		switch pc2 {
		case 0:
			require.Equal(t, buf.Len(), 0) // called before first write
			require.Equal(t, len(testData1), ii)
		case 1:
			require.Equal(t, len(testData2), ii)
		default:
			require.Fail(t, "unexpected put callback")
		}
		pc2++
	}, false)
	var pc3 int
	cw.OnPut(func(ii int) {
		switch pc3 {
		case 0:
			require.Equal(t, buf.Len(), 0) // called before first write
			require.Equal(t, len(testData1), ii)
		default:
			require.Fail(t, "unexpected put callback")
		}
		pc3++
	}, true)

	require.NoError(t, cw.Put(ctx, testCid1.KeyString(), testData1))
	require.NoError(t, cw.Put(ctx, testCid2.KeyString(), testData2))
	require.NoError(t, cw.Close())

	require.Equal(t, 2, pc1)
	require.Equal(t, 2, pc2)
	require.Equal(t, 1, pc3)
}

func randBlock() (cid.Cid, []byte) {
	data := make([]byte, 1024)
	rngLk.Lock()
	rng.Read(data)
	rngLk.Unlock()
	h, err := mh.Sum(data, mh.SHA2_512, -1)
	if err != nil {
		panic(err)
	}
	return cid.NewCidV1(cid.Raw, h), data
}
