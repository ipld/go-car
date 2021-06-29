package blockstore_test

import (
	"context"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/ipfs/go-blockservice"
	ds "github.com/ipfs/go-datastore"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunk "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer/balanced"
	"github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipld/go-car"
	"github.com/ipld/go-car/v2/blockstore"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/internal/carv1"
)

const unixfsLinksPerLevel = 1024
const unixfsChunkSize uint64 = 1 << 10

func TestBlockstore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	f, err := os.Open("testdata/test.car")
	require.NoError(t, err)
	defer f.Close()
	r, err := carv1.NewCarReader(f)
	require.NoError(t, err)
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
		require.NoError(t, err)

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

	allKeysCh, err := carb.AllKeysChan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	numKeysCh := 0
	for c := range allKeysCh {
		b, err := carb.Get(c)
		if err != nil {
			t.Fatal(err)
		}
		if !b.Cid().Equals(c) {
			t.Fatal("wrong item returned")
		}
		numKeysCh++
	}
	if numKeysCh != len(cids) {
		t.Fatal("AllKeysChan returned an unexpected amount of keys")
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

func TestBlockstorePutSameHashes(t *testing.T) {
	path := "testv2blockstore.car"
	wbs, err := blockstore.NewReadWrite(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { os.Remove(path) }()

	var blockList []blocks.Block

	appendBlock := func(data []byte, version, codec uint64) {
		c, err := cid.Prefix{
			Version:  version,
			Codec:    codec,
			MhType:   multihash.SHA2_256,
			MhLength: -1,
		}.Sum(data)
		require.NoError(t, err)

		block, err := blocks.NewBlockWithCid(data, c)
		require.NoError(t, err)

		blockList = append(blockList, block)
	}

	data1 := []byte("foo bar")
	appendBlock(data1, 0, cid.Raw)
	appendBlock(data1, 1, cid.Raw)
	appendBlock(data1, 1, cid.DagCBOR)

	data2 := []byte("foo bar baz")
	appendBlock(data2, 0, cid.Raw)
	appendBlock(data2, 1, cid.Raw)
	appendBlock(data2, 1, cid.DagCBOR)

	for i, block := range blockList {
		// Has should never error here.
		// The first block should be missing.
		// Others might not, given the duplicate hashes.
		has, err := wbs.Has(block.Cid())
		require.NoError(t, err)
		if i == 0 {
			require.False(t, has)
		}

		err = wbs.Put(block)
		require.NoError(t, err)
	}

	for _, block := range blockList {
		has, err := wbs.Has(block.Cid())
		require.NoError(t, err)
		require.True(t, has)

		got, err := wbs.Get(block.Cid())
		require.NoError(t, err)
		require.Equal(t, block.Cid(), got.Cid())
		require.Equal(t, block.RawData(), got.RawData())
	}

	err = wbs.Finalize()
	require.NoError(t, err)
}

func TestUnixFSDAGcreation(t *testing.T) {
	ctx := context.Background()

	// create a Unix FS DAG using a map based blockstore.
	bs := bstore.NewBlockstore(ds.NewMapDatastore())
	dag := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))

	// import to UnixFS
	bufferedDS := ipldformat.NewBufferedDAG(ctx, dag)

	params := helpers.DagBuilderParams{
		Maxlinks:   unixfsLinksPerLevel,
		RawLeaves:  true,
		CidBuilder: nil,
		Dagserv:    bufferedDS,
	}

	f, err := os.Open("testdata/payload.txt")
	require.NoError(t, err)
	defer f.Close()

	db, err := params.New(chunk.NewSizeSplitter(f, int64(unixfsChunkSize)))
	require.NoError(t, err)

	nd, err := balanced.Layout(db)
	require.NoError(t, err)

	err = bufferedDS.Commit()
	require.NoError(t, err)

	require.NoError(t, f.Close())

	// ----- Now that we have the root, generate the same Unix FS DAG Again with a CARv2 read-write blockstore.
	carV2Path := genWithCARv2Blockstore(t, nd.Cid())

	rdOnly, err := blockstore.OpenReadOnly(carV2Path, true)
	require.NoError(t, err)
	defer rdOnly.Close()

	// do a CARv1 traversal with the DFS selector on the CARv2 generated above.
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	allSelector := ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()

	sc := car.NewSelectiveCar(ctx, rdOnly, []car.Dag{{Root: nd.Cid(), Selector: allSelector}})
	_, err = sc.Prepare()
	require.NoError(t, err)
}

func genWithCARv2Blockstore(t *testing.T, root cid.Cid) string {
	ctx := context.Background()

	tmp, err := os.CreateTemp("", "rand")
	require.NoError(t, err)
	require.NoError(t, tmp.Close())

	rw, err := blockstore.NewReadWrite(tmp.Name(), []cid.Cid{root})
	require.NoError(t, err)

	bsvc := blockservice.New(rw, offline.Exchange(rw))
	dag := merkledag.NewDAGService(bsvc)
	// import to UnixFS
	bufferedDS := ipldformat.NewBufferedDAG(ctx, dag)

	params := helpers.DagBuilderParams{
		Maxlinks:   unixfsLinksPerLevel,
		RawLeaves:  true,
		CidBuilder: nil,
		Dagserv:    bufferedDS,
	}

	f, err := os.Open("testdata/payload.txt")
	require.NoError(t, err)

	db, err := params.New(chunk.NewSizeSplitter(f, int64(unixfsChunkSize)))
	require.NoError(t, err)

	// TODO: The below lines fail with "not found".
	_, err = balanced.Layout(db)
	require.NoError(t, err)

	err = bufferedDS.Commit()
	require.NoError(t, err)

	require.NoError(t, rw.Finalize())

	return tmp.Name()
}
