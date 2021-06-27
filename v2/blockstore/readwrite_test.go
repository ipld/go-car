package blockstore_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/ipfs/go-blockservice"
	ds "github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunk "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer/balanced"
	"github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ipld/go-car/v2/blockstore"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/internal/carv1"
)

const unixfsChunkSize uint64 = 1 << 10
const unixfsLinksPerLevel = 1024

func TestFailure(t *testing.T) {
	// copy a UnixFS DAG to a CARv2 file.
	// Blockstore dosen't work as expected.

	f, err := os.Open("testdata/payload.txt")
	require.NoError(t, err)

	ds := bstore.NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
	bsvc := blockservice.New(ds, offline.Exchange(ds))
	dag := merkledag.NewDAGService(bsvc)

	params := helpers.DagBuilderParams{
		Maxlinks:   unixfsLinksPerLevel,
		RawLeaves:  true,
		CidBuilder: nil,
		Dagserv:    dag,
	}

	db, err := params.New(chunk.NewSizeSplitter(f, int64(unixfsChunkSize)))
	require.NoError(t, err)

	root, err := balanced.Layout(db)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Try to create the same DAG with a CARv2 blockstore
	f, err = os.Open("testdata/payload.txt")
	require.NoError(t, err)

	f2, err := os.CreateTemp("", "rand")
	require.NoError(t, err)
	defer f2.Close()
	defer os.Remove(f2.Name())
	rw, err := blockstore.NewReadWrite(f2.Name(), []cid.Cid{root.Cid()})
	require.NoError(t, err)
	bsvc = blockservice.New(rw, offline.Exchange(rw))
	dag = merkledag.NewDAGService(bsvc)
	// import to UnixFS
	params = helpers.DagBuilderParams{
		Maxlinks:   unixfsLinksPerLevel,
		RawLeaves:  true,
		CidBuilder: nil,
		Dagserv:    dag,
	}
	db, err = params.New(chunk.NewSizeSplitter(f, int64(unixfsChunkSize)))
	require.NoError(t, err)
	_, err = balanced.Layout(db)
	// TODO Fails with the block not found error, says not in index.
	//	require.NoError(t, err)

	// TODO This also fails.
	//copyDAGToCARv2(t, root.Cid(), ds)

	// TODO This also fails.
	copyDAGToCARv2ManuallyAddRoot(t, root.Cid(), ds)

}

func copyDAGToCARv2(t *testing.T, rootCid cid.Cid, bs bstore.Blockstore) {
	fmt.Println("\n root cid is", rootCid)

	// create a new read-write blockstore.
	f, err := os.CreateTemp("", rootCid.String())
	require.NoError(t, err)
	rw, err := blockstore.NewReadWrite(f.Name(), []cid.Cid{rootCid})
	require.NoError(t, err)

	// copy blocks from a given blockstore to a read-write blockstore.
	ch, err := bs.AllKeysChan(context.Background())
	require.NoError(t, err)
	for k := range ch {
		blk, err := bs.Get(k)
		require.NoError(t, err)
		require.NoError(t, rw.Put(blk))
	}

	// finalize the read-write blockstore.
	require.NoError(t, rw.Finalize())

	// verify all blocks have been written
	rdOnly, err := blockstore.OpenReadOnly(f.Name(), false)
	require.NoError(t, err)

	ch, err = bs.AllKeysChan(context.Background())
	require.NoError(t, err)
	for k := range ch {
		has, err := rdOnly.Has(k)
		require.NoError(t, err, k.String())
		require.True(t, has, k.String())

		blk, err := rdOnly.Get(k)
		require.NoError(t, err, k.String())
		blk2, err := bs.Get(k)
		require.NoError(t, err)
		require.Equal(t, blk.RawData(), blk2.RawData())
	}

	// somehow, the root Cid isn't there in bs.AllKeysChan even though has says yes here-> there is a bug in
	// bstore.NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
	has, err := bs.Has(rootCid)
	require.NoError(t, err)
	require.True(t, has)

	blk, err := bs.Get(rootCid)
	require.NoError(t, err)
	require.NotEmpty(t, blk.String())

	// THIS FAILS !
	has, err = rdOnly.Has(rootCid)
	require.True(t, has)
	require.NoError(t, err)
	blk2, err := rdOnly.Get(rootCid)
	require.NoError(t, err)
	require.NotEmpty(t, blk2.String())

	require.NoError(t, rdOnly.Close())
}

// This also fails mysteriously
func copyDAGToCARv2ManuallyAddRoot(t *testing.T, rootCid cid.Cid, bs bstore.Blockstore) {
	fmt.Println("\n root cid is", rootCid)

	// create a new read-write blockstore.
	f, err := os.CreateTemp("", rootCid.String())
	require.NoError(t, err)
	rw, err := blockstore.NewReadWrite(f.Name(), []cid.Cid{rootCid})
	require.NoError(t, err)

	// manually write the root block since that fails.
	blk, err := bs.Get(rootCid)
	require.NoError(t, err)
	require.NoError(t, rw.Put(blk))
	blk2, err := rw.Get(rootCid)
	require.NoError(t, err)
	require.Equal(t, blk.RawData(), blk2.RawData())

	// copy blocks from a given blockstore to a read-write blockstore.
	ch, err := bs.AllKeysChan(context.Background())
	require.NoError(t, err)
	for k := range ch {
		blk, err := bs.Get(k)
		require.NoError(t, err)
		require.NoError(t, rw.Put(blk))
	}

	// finalize the read-write blockstore.
	require.NoError(t, rw.Finalize())

	// verify all blocks have been written
	rdOnly, err := blockstore.OpenReadOnly(f.Name(), false)
	require.NoError(t, err)

	ch, err = bs.AllKeysChan(context.Background())
	require.NoError(t, err)
	for k := range ch {
		has, err := rdOnly.Has(k)
		require.NoError(t, err, k.String())
		// TODO -> Fails here even though we wrote the block above
		require.True(t, has, k.String())

		blk, err := rdOnly.Get(k)
		require.NoError(t, err, k.String())
		blk2, err := bs.Get(k)
		require.NoError(t, err)
		require.Equal(t, blk.RawData(), blk2.RawData())
	}
}

func TestBlockstore(t *testing.T) {
	f, err := os.Open("testdata/test.car")
	assert.NoError(t, err)
	defer f.Close()
	r, err := carv1.NewCarReader(f)
	assert.NoError(t, err)
	path := "testv2blockstore.car"
	ingester, err := blockstore.NewReadWrite(path, r.Header.Roots)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.Remove(path)
	}()

	cids := make([]cid.Cid, 0)
	for {
		b, err := r.Next()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)

		if err := ingester.Put(b); err != nil {
			t.Fatal(err)
		}
		cids = append(cids, b.Cid())

		// try reading a random one:
		candidate := cids[rand.Intn(len(cids))]
		if has, err := ingester.Has(candidate); !has || err != nil {
			t.Fatalf("expected to find %s but didn't: %s", candidate, err)
		}
	}

	for _, c := range cids {
		b, err := ingester.Get(c)
		if err != nil {
			t.Fatal(err)
		}
		if !b.Cid().Equals(c) {
			t.Fatal("wrong item returned")
		}
	}

	if err := ingester.Finalize(); err != nil {
		t.Fatal(err)
	}
	carb, err := blockstore.OpenReadOnly(path, false)
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range cids {
		b, err := carb.Get(c)
		if err != nil {
			t.Fatal(err)
		}
		if !b.Cid().Equals(c) {
			t.Fatal("wrong item returned")
		}
	}
}
