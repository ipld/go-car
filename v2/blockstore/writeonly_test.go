package blockstore_test

import (
	"context"
	"crypto/sha512"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	blocks "github.com/ipfs/go-libipfs/blocks"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamingWriteOnlyV1Blockstore(t *testing.T) {

	testCases := []struct {
		name                string
		originalCar         string
		opts                []carv2.Option
		storingIdentityCids bool
	}{
		{
			name:                "store identity CIDs",
			originalCar:         "../testdata/sample-v1.car",
			opts:                []carv2.Option{carv2.StoreIdentityCIDs(true)},
			storingIdentityCids: true,
		},
		{
			name:        "do not store identity CIDs",
			originalCar: "../testdata/sample-v1-noidentity.car",
			opts:        []carv2.Option{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			f, err := os.Open(tc.originalCar)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, f.Close()) })
			r, err := carv1.NewCarReader(f)
			require.NoError(t, err)

			path := filepath.Join(t.TempDir(), "writeonlyv1.car")
			out, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
			require.NoError(t, err)
			wocar, err := blockstore.CreateWriteOnlyV1(&onlyWriter{out}, r.Header.Roots, tc.opts...)
			require.NoError(t, err)

			cids := make(map[cid.Cid]int, 0)
			var idCidCount int
			for {
				b, err := r.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)

				err = wocar.Put(ctx, b)
				require.NoError(t, err)
				cids[b.Cid()] = len(b.RawData())

				// try reading a random one:
				candIndex := rng.Intn(len(cids))
				var candidate cid.Cid
				for c := range cids {
					if candIndex == 0 {
						candidate = c
						break
					}
					candIndex--
				}
				has, err := wocar.Has(ctx, candidate)
				require.NoError(t, err)
				require.True(t, has)

				has, err = wocar.Has(ctx, randCid())
				require.NoError(t, err)
				require.False(t, has)

				has, err = wocar.Has(ctx, randIdentityCid())
				require.NoError(t, err)
				require.Equal(t, !tc.storingIdentityCids, has)

				dmh, err := multihash.Decode(b.Cid().Hash())
				require.NoError(t, err)
				if dmh.Code == multihash.IDENTITY {
					idCidCount++
				}
			}

			if !tc.storingIdentityCids {
				// should not show up in list
				err := wocar.Put(ctx, randIdentityBlock())
				require.NoError(t, err)
			}

			b, err := wocar.Get(ctx, randCid())
			require.ErrorIs(t, err, blockstore.ErrWriteOnly)
			require.Nil(t, b)

			err = wocar.DeleteBlock(ctx, randCid())
			require.ErrorIs(t, err, blockstore.ErrWriteOnly)

			require.NoError(t, out.Close())

			for c, size := range cids {
				s, err := wocar.GetSize(ctx, c)
				require.NoError(t, err)
				if s != size {
					t.Fatal("GetSize returned wrong size")
				}
				has, err := wocar.Has(ctx, c)
				require.NoError(t, err)
				require.True(t, has)
			}

			allKeysCh, err := wocar.AllKeysChan(ctx)
			require.NoError(t, err)
			numKeysCh := 0
			for c := range allKeysCh {
				// cids map contains this cid
				_, has := cids[c]
				require.True(t, has)
				numKeysCh++
			}
			require.Equal(t, len(cids), numKeysCh)

			// open it as a ReadOnly to check it out
			robs, err := blockstore.OpenReadOnly(path, tc.opts...)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, robs.Close()) })

			robsRoots, err := robs.Roots()
			require.NoError(t, err)
			require.Equal(t, 1, len(robsRoots))
			require.Equal(t, r.Header.Roots[0], robsRoots[0])

			allKeysCh, err = robs.AllKeysChan(ctx)
			require.NoError(t, err)
			numKeysCh = 0
			for c := range allKeysCh {
				b, err := robs.Get(ctx, c)
				require.NoError(t, err)
				if !b.Cid().Equals(c) {
					t.Fatal("wrong item returned")
				}
				numKeysCh++
			}
			expectedCidCount := len(cids)
			if !tc.storingIdentityCids {
				expectedCidCount -= idCidCount
			}
			require.Equal(t, expectedCidCount, numKeysCh, "AllKeysChan returned an unexpected amount of keys; expected %v but got %v", expectedCidCount, numKeysCh)

			for c, size := range cids {
				s, err := wocar.GetSize(ctx, c)
				require.NoError(t, err)
				if s != size {
					t.Fatal("GetSize returned wrong size")
				}
			}

			wrote, err := os.Open(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wrote.Close()) })
			_, err = wrote.Seek(0, io.SeekStart)
			require.NoError(t, err)
			hasher := sha512.New()
			gotWritten, err := io.Copy(hasher, wrote)
			require.NoError(t, err)
			gotSum := hasher.Sum(nil)

			hasher.Reset()
			originalCarV1, err := os.Open(tc.originalCar)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, originalCarV1.Close()) })
			wantWritten, err := io.Copy(hasher, originalCarV1)
			require.NoError(t, err)
			wantSum := hasher.Sum(nil)

			require.Equal(t, wantWritten, gotWritten)
			require.Equal(t, wantSum, gotSum)
		})
	}
}

func randCid() cid.Cid {
	b := make([]byte, 32)
	mh, _ := multihash.Encode(b, multihash.SHA2_256)
	return cid.NewCidV1(cid.DagProtobuf, mh)
}

func randIdentityCid() cid.Cid {
	b := make([]byte, 32)
	mh, _ := multihash.Encode(b, multihash.IDENTITY)
	return cid.NewCidV1(cid.Raw, mh)
}

func randIdentityBlock() blocks.Block {
	b := make([]byte, 32)
	mh, _ := multihash.Encode(b, multihash.IDENTITY)
	blk, err := blocks.NewBlockWithCid(b, cid.NewCidV1(cid.Raw, mh))
	if err != nil {
		panic(err)
	}
	return blk
}

type onlyWriter struct {
	w io.Writer
}

func (w *onlyWriter) Write(p []byte) (int, error) {
	return w.w.Write(p)
}
