package car_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ipfs/go-cidutil"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	mh "github.com/multiformats/go-multihash"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-fil-markets/stores"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/stretchr/testify/require"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
)

var DefaultHashFunction = uint64(mh.BLAKE2B_MIN + 31)

const UnixfsChunkSize uint64 = 1 << 20
const UnixfsLinksPerLevel = 1024

func ExampleWrapV1File() {
	// We have a sample CARv1 file.
	// Wrap it as-is in a CARv2, with an index.
	// Writing the result to testdata allows reusing that file in other tests,
	// and also helps ensure that the result is deterministic.
	src := "testdata/sample-v1.car"
	tdir, err := ioutil.TempDir(os.TempDir(), "example-*")
	if err != nil {
		panic(err)
	}
	dst := filepath.Join(tdir, "wrapped-v2.car")
	if err := carv2.WrapV1File(src, dst); err != nil {
		panic(err)
	}

	// Open our new CARv2 file and show some info about it.
	cr, err := carv2.OpenReader(dst)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := cr.Close(); err != nil {
			panic(err)
		}
	}()

	roots, err := cr.Roots()
	if err != nil {
		panic(err)
	}
	fmt.Println("Roots:", roots)
	fmt.Println("Has index:", cr.Header.HasIndex())

	// Verify that the CARv1 remains exactly the same.
	orig, err := ioutil.ReadFile(src)
	if err != nil {
		panic(err)
	}
	inner, err := ioutil.ReadAll(cr.DataReader())
	if err != nil {
		panic(err)
	}
	fmt.Println("Inner CARv1 is exactly the same:", bytes.Equal(orig, inner))

	// Verify that the CARv2 works well with its index.
	bs, err := blockstore.OpenReadOnly(dst)
	if err != nil {
		panic(err)
	}
	fmt.Println(bs.Get(roots[0]))

	// Output:
	// Roots: [bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy]
	// Has index: true
	// Inner CARv1 is exactly the same: true
	// [Block bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy] <nil>
}

// ExampleNewBlockReader instantiates a new BlockReader for a CARv1 file and its wrapped CARv2
// version. For each file, it prints the version, the root CIDs and the first five block CIDs.
// Note, the roots and first five block CIDs are identical in both files since both represent the
// same root CIDs and data blocks.
func ExampleNewBlockReader() {
	for _, path := range []string{
		"testdata/sample-v1.car",
		"testdata/sample-wrapped-v2.car",
	} {
		fmt.Println("File:", path)
		f, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		br, err := carv2.NewBlockReader(f)
		if err != nil {
			panic(err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				panic(err)
			}
		}()
		fmt.Println("Version:", br.Version)
		fmt.Println("Roots:", br.Roots)
		fmt.Println("First 5 block CIDs:")
		for i := 0; i < 5; i++ {
			bl, err := br.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}
			fmt.Printf("\t%v\n", bl.Cid())
		}
	}
	// Output:
	// File: testdata/sample-v1.car
	// Version: 1
	// Roots: [bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy]
	// First 5 block CIDs:
	// 	bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy
	// 	bafy2bzaceaycv7jhaegckatnncu5yugzkrnzeqsppzegufr35lroxxnsnpspu
	// 	bafy2bzaceb62wdepofqu34afqhbcn4a7jziwblt2ih5hhqqm6zitd3qpzhdp4
	// 	bafy2bzaceb3utcspm5jqcdqpih3ztbaztv7yunzkiyfq7up7xmokpxemwgu5u
	// 	bafy2bzacedjwekyjresrwjqj4n2r5bnuuu3klncgjo2r3slsp6wgqb37sz4ck
	// File: testdata/sample-wrapped-v2.car
	// Version: 2
	// Roots: [bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy]
	// First 5 block CIDs:
	// 	bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy
	// 	bafy2bzaceaycv7jhaegckatnncu5yugzkrnzeqsppzegufr35lroxxnsnpspu
	// 	bafy2bzaceb62wdepofqu34afqhbcn4a7jziwblt2ih5hhqqm6zitd3qpzhdp4
	// 	bafy2bzaceb3utcspm5jqcdqpih3ztbaztv7yunzkiyfq7up7xmokpxemwgu5u
	// 	bafy2bzacedjwekyjresrwjqj4n2r5bnuuu3klncgjo2r3slsp6wgqb37sz4ck
}

func TestFail(t *testing.T) {
	ctx := context.Background()

	inputPath, _ := genInputFile(t)
	defer os.Remove(inputPath) //nolint:errcheck

	dst := newTmpFile(t)
	defer os.Remove(dst) //nolint:errcheck

	root, err := createUnixFSFilestore(ctx, inputPath, dst)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, root)

	f, err := os.Open(dst)
	require.NoError(t, err)

	reader, err := carv2.NewBlockReader(f)
	require.NoError(t, err)
	var i int
	for {
		next, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		t.Logf("\t%v\n", next.Cid())
		i++
	}
	t.Logf("CID count:   %d\n", i)
	t.Logf("CAR version: %d\n", reader.Version)
	t.Logf("CID roots:   %v\n", reader.Roots)

	// convert the CARv2 to a normal file again and ensure the contents match
	fs, err := stores.ReadOnlyFilestore(dst)
	require.NoError(t, err)
	defer fs.Close() //nolint:errcheck

	dags := merkledag.NewDAGService(blockservice.New(fs, offline.Exchange(fs)))

	nd, err := dags.Get(ctx, root)
	require.NoError(t, err)

	file, err := unixfile.NewUnixfsFile(ctx, dags, nd)
	require.NoError(t, err)

	tmpOutput := newTmpFile(t)
	defer os.Remove(tmpOutput) //nolint:errcheck
	require.NoError(t, files.WriteTo(file, tmpOutput))
}

// createUnixFSFilestore takes a standard file whose path is src, forms a UnixFS DAG, and
// writes a CARv2 file with positional mapping (backed by the go-filestore library).
func createUnixFSFilestore(ctx context.Context, srcPath string, dstPath string) (cid.Cid, error) {
	// This method uses a two-phase approach with a staging CAR blockstore and
	// a final CAR blockstore.
	//
	// This is necessary because of https://github.com/ipld/go-car/issues/196
	//
	// TODO: do we need to chunk twice? Isn't the first output already in the
	//  right order? Can't we just copy the CAR file and replace the header?

	src, err := os.Open(srcPath)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to open input file: %w", err)
	}
	defer src.Close() //nolint:errcheck

	stat, err := src.Stat()
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to stat file :%w", err)
	}

	file, err := files.NewReaderPathFile(srcPath, src, stat)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to create reader path file: %w", err)
	}

	f, err := ioutil.TempFile("", "")
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to create temp file: %w", err)
	}
	_ = f.Close() // close; we only want the path.

	tmp := f.Name()
	defer os.Remove(tmp) //nolint:errcheck

	// Step 1. Compute the UnixFS DAG and write it to a CARv2 file to get
	// the root CID of the DAG.
	fstore, err := stores.ReadWriteFilestore(tmp)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to create temporary filestore: %w", err)
	}

	finalRoot1, err := buildUnixFS(ctx, file, fstore, true)
	if err != nil {
		_ = fstore.Close()
		return cid.Undef, xerrors.Errorf("failed to import file to store to compute root: %w", err)
	}

	if err := fstore.Close(); err != nil {
		return cid.Undef, xerrors.Errorf("failed to finalize car filestore: %w", err)
	}

	// Step 2. We now have the root of the UnixFS DAG, and we can write the
	// final CAR for real under `dst`.
	bs, err := stores.ReadWriteFilestore(dstPath, finalRoot1)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to create a carv2 read/write filestore: %w", err)
	}

	// rewind file to the beginning.
	if _, err := src.Seek(0, 0); err != nil {
		return cid.Undef, xerrors.Errorf("failed to rewind file: %w", err)
	}

	finalRoot2, err := buildUnixFS(ctx, file, bs, true)
	if err != nil {
		_ = bs.Close()
		return cid.Undef, xerrors.Errorf("failed to create UnixFS DAG with carv2 blockstore: %w", err)
	}

	if err := bs.Close(); err != nil {
		return cid.Undef, xerrors.Errorf("failed to finalize car blockstore: %w", err)
	}

	if finalRoot1 != finalRoot2 {
		return cid.Undef, xerrors.New("roots do not match")
	}

	return finalRoot1, nil
}

func newTmpFile(t *testing.T) string {
	f, err := os.CreateTemp("", "")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func genInputFile(t *testing.T) (filepath string, contents []byte) {
	s := strings.Repeat("abcde", 100)
	tmp, err := os.CreateTemp("", "")
	require.NoError(t, err)
	_, err = io.Copy(tmp, strings.NewReader(s))
	require.NoError(t, err)
	require.NoError(t, tmp.Close())
	return tmp.Name(), []byte(s)
}

// buildUnixFS builds a UnixFS DAG out of the supplied reader,
// and imports the DAG into the supplied service.
func buildUnixFS(ctx context.Context, reader io.Reader, into bstore.Blockstore, filestore bool) (cid.Cid, error) {
	b, err := unixFSCidBuilder()
	if err != nil {
		return cid.Undef, err
	}

	bsvc := blockservice.New(into, offline.Exchange(into))
	dags := merkledag.NewDAGService(bsvc)
	bufdag := ipld.NewBufferedDAG(ctx, dags)

	params := ihelper.DagBuilderParams{
		Maxlinks:   UnixfsLinksPerLevel,
		RawLeaves:  true,
		CidBuilder: b,
		Dagserv:    bufdag,
		NoCopy:     filestore,
	}

	db, err := params.New(chunker.NewSizeSplitter(reader, int64(UnixfsChunkSize)))
	if err != nil {
		return cid.Undef, err
	}
	nd, err := balanced.Layout(db)
	if err != nil {
		return cid.Undef, err
	}

	if err := bufdag.Commit(); err != nil {
		return cid.Undef, err
	}

	return nd.Cid(), nil
}

func unixFSCidBuilder() (cid.Builder, error) {
	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize UnixFS CID Builder: %w", err)
	}
	prefix.MhType = DefaultHashFunction
	b := cidutil.InlineBuilder{
		Builder: prefix,
		Limit:   126,
	}
	return b, nil
}
