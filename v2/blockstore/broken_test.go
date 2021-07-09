package blockstore

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
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
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

const unixfsChunkSize uint64 = 1 << 10
const unixfsLinksPerLevel = 1024

func TestCarIndexBug(t *testing.T) {
	ctx := context.Background()

	// Generate a CARv2 file from a normal file
	pieceLink, carV2FilePath := loadUnixFSFile(t, ctx, "testdata/lorem.txt")
	c, ok := pieceLink.(cidlink.Link)
	require.True(t, ok)
	payloadCID := c.Cid

	// Do a Selective CARv1 traversal on the CARv2 file to get a
	// deterministic CARv1 and write the deterministic CARv1 to `carBuf`.
	rdOnly, err := OpenReadOnly(carV2FilePath)
	require.NoError(t, err)
	sc := car.NewSelectiveCar(ctx, rdOnly, []car.Dag{{Root: payloadCID, Selector: shared.AllSelector()}})
	prepared, err := sc.Prepare()
	require.NoError(t, err)
	carBuf := new(bytes.Buffer)
	require.NoError(t, prepared.Write(carBuf))
	require.NoError(t, rdOnly.Close())

	// open the CARv2 file and detach the Index from it.
	f, err := os.Open(carV2FilePath)
	require.NoError(t, err)
	defer f.Close()
	idx, err := carv2.ReadOrGenerateIndex(f)
	require.NoError(t, err)

	// copy the deterministic CARv1 we got above to a file f3.
	f2, err := os.CreateTemp("", "")
	require.NoError(t, err)
	defer os.Remove(f2.Name())
	defer f2.Close()
	_, err = io.Copy(f2, carBuf)
	require.NoError(t, err)
	require.NoError(t, f2.Close())
	f3, err := os.Open(f2.Name())
	require.NoError(t, err)

	// Open a Read Only Blockstore using the deterministic CARv1 payload and the Index we detached above.
	bs, err := NewReadOnly(f3, idx)
	require.NoError(t, err)

	ch, err := bs.AllKeysChan(ctx)
	require.NoError(t, err)

	for c := range ch {
		t.Logf("Get %s", c)
		// TODO FAILS HERE WITH AN `ERRNOTFOUND`.
		_, err := bs.Get(c)
		require.NoError(t, err)
	}
}

func loadUnixFSFile(t *testing.T, ctx context.Context, fixturesPath string) (ipld.Link, string) {
	f, err := os.Open(fixturesPath)
	require.NoError(t, err)

	dstore := dss.MutexWrap(datastore.NewMapDatastore())
	bs := bstore.NewBlockstore(dstore)
	dagService := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))
	file := files.NewReaderFile(f)

	// import to UnixFS
	bufferedDS := ipldformat.NewBufferedDAG(ctx, dagService)

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

	// save the original files bytes
	require.NoError(t, f.Close())

	// Create a UnixFS DAG again AND generate a CARv2 file using a CARv2 read-write blockstore now that we have the root.
	carV2Path := genWithCARv2Blockstore(t, fixturesPath, nd.Cid())

	return cidlink.Link{Cid: nd.Cid()}, carV2Path
}

func genWithCARv2Blockstore(t *testing.T, fPath string, root cid.Cid) string {
	ctx := context.Background()
	tmp, err := os.CreateTemp("", "rand")
	require.NoError(t, err)
	require.NoError(t, tmp.Close())

	rw, err := NewReadWrite(tmp.Name(), []cid.Cid{root}, WithCidDeduplication)
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

	nd, err := balanced.Layout(db)
	require.NoError(t, err)

	err = bufferedDS.Commit()
	require.NoError(t, err)

	require.NoError(t, rw.Finalize())
	require.Equal(t, root, nd.Cid())

	// return the path of the CARv2 file.
	return tmp.Name()
}
