package car_test

import (
	"bytes"
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/bsadapter"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"

	_ "github.com/ipld/go-codec-dagpb"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
)

func TestPrepareTraversal(t *testing.T) {
	from, err := blockstore.OpenReadOnly("testdata/sample-unixfs-v2.car")
	require.NoError(t, err)
	ls := cidlink.DefaultLinkSystem()
	bsa := bsadapter.Adapter{Wrapped: from}
	ls.SetReadStorage(&bsa)

	rts, _ := from.Roots()
	writer, err := car.NewSelectiveWriter(context.Background(), &ls, rts[0], selectorparse.CommonSelector_ExploreAllRecursively)
	require.NoError(t, err)

	buf := bytes.Buffer{}
	n, err := writer.WriteTo(&buf)
	require.NoError(t, err)
	require.Equal(t, int64(len(buf.Bytes())), n)

	fi, _ := os.Stat("testdata/sample-unixfs-v2.car")
	require.Equal(t, fi.Size(), n)
}

func TestFileTraversal(t *testing.T) {
	from, err := blockstore.OpenReadOnly("testdata/sample-unixfs-v2.car")
	require.NoError(t, err)
	ls := cidlink.DefaultLinkSystem()
	bsa := bsadapter.Adapter{Wrapped: from}
	ls.SetReadStorage(&bsa)

	rts, _ := from.Roots()
	outDir := filepath.Join(t.TempDir(), "car-file-traversal.car")
	err = car.TraverseToFile(context.Background(), &ls, rts[0], selectorparse.CommonSelector_ExploreAllRecursively, path.Join(outDir, "out.car"))
	require.NoError(t, err)

	require.FileExists(t, path.Join(outDir, "out.car"))

	fa, _ := os.Stat("testdata/sample-unixfs-v2.car")
	fb, _ := os.Stat(path.Join(outDir, "out.car"))
	require.Equal(t, fa.Size(), fb.Size())
}
