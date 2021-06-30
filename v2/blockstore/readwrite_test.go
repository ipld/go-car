package blockstore_test

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-blockservice"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunk "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer/balanced"
	"github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipld/go-car"
	carv2 "github.com/ipld/go-car/v2"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	"github.com/ipld/go-car/v2/blockstore"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/internal/carv1"
)

const unixfsChunkSize uint64 = 1 << 10
const unixfsLinksPerLevel = 1024

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

func TestBlockstoreConcurrentUse(t *testing.T) {
	path := "testv2blockstore.car"
	wbs, err := blockstore.NewReadWrite(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { os.Remove(path) }()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))

		wg.Add(1)
		go func() {
			defer wg.Done()

			c, err := cid.Prefix{
				Version:  1,
				Codec:    cid.Raw,
				MhType:   multihash.SHA2_256,
				MhLength: -1,
			}.Sum(data)
			require.NoError(t, err)

			block, err := blocks.NewBlockWithCid(data, c)
			require.NoError(t, err)

			has, err := wbs.Has(block.Cid())
			require.NoError(t, err)
			require.False(t, has)

			err = wbs.Put(block)
			require.NoError(t, err)

			got, err := wbs.Get(block.Cid())
			require.NoError(t, err)
			require.Equal(t, data, got.RawData())
		}()
	}
	wg.Wait()
}

// The CARv2 file for a UnixFS DAG that has duplicates should NOT have duplicates.
func TestDeDup(t *testing.T) {
	// generate a CARv2 file from the "testdata/duplicate_blocks.txt" and also get the "inmemory" blockstore that was
	// used to create the UnixFS DAG in the first pass when we didn't have it's root.
	root, CARv2Path, inmemory := GenCARv2FromNormalFile(t, "testdata/duplicate_blocks.txt")
	require.NotEmpty(t, CARv2Path)
	defer os.Remove(CARv2Path)

	// Get a reader over the CARv1 payload of the CARv2 file.
	// and iterate over the CARv1 payload to ensure there are no duplicates in it.
	v2r, err := carv2.NewReaderMmap(CARv2Path)
	require.NoError(t, err)
	defer v2r.Close()
	cr, err := car.NewCarReader(v2r.CarV1Reader())
	require.NoError(t, err)
	seen := make(map[cid.Cid]struct{})
	for {
		b, err := cr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		_, ok := seen[b.Cid()]
		// TODO This line fails as CARv2 has duplicate blocks.
		require.Falsef(t, ok, "already seen cid %s", b.Cid())
		seen[b.Cid()] = struct{}{}
	}

	// A CARv1 traversal over the UnixFS DAG using the inmemory blockstore wll return all the de-duped blocks ->
	// should be the same as what the CARv1 reader above returned.
	seen2 := make(map[cid.Cid]struct{})
	var mu sync.Mutex
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel := ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).
		Node()

	sc := car.NewSelectiveCar(context.Background(), inmemory, []car.Dag{
		{
			Root:     root,
			Selector: sel,
		},
	})

	require.NoError(t, sc.Write(ioutil.Discard, func(b car.Block) error {
		mu.Lock()
		defer mu.Unlock()

		if _, ok := seen2[b.BlockCID]; ok {
			err = xerrors.Errorf("already seen cid %s", b.BlockCID)
		}

		seen2[b.BlockCID] = struct{}{}

		return nil
	}))

	mu.Lock()
	defer mu.Unlock()

	require.NoError(t, err)
	// both maps should have the same blocks
	require.True(t, reflect.DeepEqual(seen, seen2))
}

// GenCARv2FromNormalFile generates a CARv2 file from a "normal" i.e. non-CAR file and returns the file path.
func GenCARv2FromNormalFile(t *testing.T, normalFilePath string) (root cid.Cid, carV2FilePath string, blockstore bstore.Blockstore) {
	ctx := context.Background()

	f, err := os.Open(normalFilePath)
	require.NoError(t, err)
	file := files.NewReaderFile(f)
	bs := bstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	dag := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))

	// import to UnixFS
	bufferedDS := ipldformat.NewBufferedDAG(ctx, dag)

	params := helpers.DagBuilderParams{
		Maxlinks:   unixfsLinksPerLevel,
		RawLeaves:  true,
		CidBuilder: nil,
		Dagserv:    bufferedDS,
	}

	db, err := params.New(chunk.NewSizeSplitter(file, int64(unixfsChunkSize)))
	require.NoError(t, err)

	nd, err := balanced.Layout(db)
	require.NoError(t, err)

	err = bufferedDS.Commit()
	require.NoError(t, err)
	require.NoError(t, file.Close())

	// Create a UnixFS DAG again AND generate a CARv2 file using a CARv2 read-write blockstore now that we have the root.
	carV2Path := genWithCARv2Blockstore(t, normalFilePath, nd.Cid())

	return nd.Cid(), carV2Path, bs
}

func genWithCARv2Blockstore(t *testing.T, fPath string, root cid.Cid) string {
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

	f, err := os.Open(fPath)
	require.NoError(t, err)

	db, err := params.New(chunk.NewSizeSplitter(f, int64(unixfsChunkSize)))
	require.NoError(t, err)

	// TODO: The below lines fail with "not found".
	nd, err := balanced.Layout(db)
	require.NoError(t, err)

	err = bufferedDS.Commit()
	require.NoError(t, err)

	require.NoError(t, rw.Finalize())
	require.Equal(t, root, nd.Cid())

	// return the path of the CARv2 file.
	return tmp.Name()
}
