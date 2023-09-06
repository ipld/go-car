package car_test

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/internal/carv1"
	mh "github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
	"github.com/stretchr/testify/require"
)

func TestBlockReaderFailsOnUnknownVersion(t *testing.T) {
	r := requireReaderFromPath(t, "testdata/sample-rootless-v42.car")
	_, err := carv2.NewBlockReader(r)
	require.EqualError(t, err, "invalid car version: 42")
}

func TestBlockReaderFailsOnCorruptPragma(t *testing.T) {
	r := requireReaderFromPath(t, "testdata/sample-corrupt-pragma.car")
	_, err := carv2.NewBlockReader(r)
	require.EqualError(t, err, "unexpected EOF")
}

func TestBlockReader_WithCarV1Consistency(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		zerLenAsEOF bool
		wantVersion uint64
	}{
		{
			name:        "CarV1WithoutZeroLengthSection",
			path:        "testdata/sample-v1.car",
			wantVersion: 1,
		},
		{
			name:        "CarV1WithZeroLenSection",
			path:        "testdata/sample-v1-with-zero-len-section.car",
			zerLenAsEOF: true,
			wantVersion: 1,
		},
		{
			name:        "AnotherCarV1WithZeroLenSection",
			path:        "testdata/sample-v1-with-zero-len-section2.car",
			zerLenAsEOF: true,
			wantVersion: 1,
		},
		{
			name:        "CarV1WithZeroLenSectionWithoutOption",
			path:        "testdata/sample-v1-with-zero-len-section.car",
			wantVersion: 1,
		},
		{
			name:        "AnotherCarV1WithZeroLenSectionWithoutOption",
			path:        "testdata/sample-v1-with-zero-len-section2.car",
			wantVersion: 1,
		},
		{
			name:        "CorruptCarV1",
			path:        "testdata/sample-v1-tailing-corrupt-section.car",
			wantVersion: 1,
		},
		{
			name:        "CarV2WrappingV1",
			path:        "testdata/sample-wrapped-v2.car",
			wantVersion: 2,
		},
		{
			name:        "CarV2ProducedByBlockstore",
			path:        "testdata/sample-rw-bs-v2.car",
			wantVersion: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := requireReaderFromPath(t, tt.path)
			subject, err := carv2.NewBlockReader(r, carv2.ZeroLengthSectionAsEOF(tt.zerLenAsEOF))
			require.NoError(t, err)

			require.Equal(t, tt.wantVersion, subject.Version)

			var wantReader *carv1.CarReader
			switch tt.wantVersion {
			case 1:
				wantReader = requireNewCarV1ReaderFromV1File(t, tt.path, tt.zerLenAsEOF)
			case 2:
				wantReader = requireNewCarV1ReaderFromV2File(t, tt.path, tt.zerLenAsEOF)
			default:
				require.Failf(t, "invalid test-case", "unknown wantVersion %v", tt.wantVersion)
			}
			require.Equal(t, wantReader.Header.Roots, subject.Roots)

			for {
				gotBlock, gotErr := subject.Next()
				wantBlock, wantErr := wantReader.Next()
				require.Equal(t, wantBlock, gotBlock)
				require.Equal(t, wantErr, gotErr)
				if gotErr == io.EOF {
					break
				}
			}
		})
		t.Run(tt.name+"-skipping-reads", func(t *testing.T) {
			r := requireReaderFromPath(t, tt.path)
			subject, err := carv2.NewBlockReader(r, carv2.ZeroLengthSectionAsEOF(tt.zerLenAsEOF))
			require.NoError(t, err)

			require.Equal(t, tt.wantVersion, subject.Version)

			var wantReader *carv1.CarReader
			switch tt.wantVersion {
			case 1:
				wantReader = requireNewCarV1ReaderFromV1File(t, tt.path, tt.zerLenAsEOF)
			case 2:
				wantReader = requireNewCarV1ReaderFromV2File(t, tt.path, tt.zerLenAsEOF)
			default:
				require.Failf(t, "invalid test-case", "unknown wantVersion %v", tt.wantVersion)
			}
			require.Equal(t, wantReader.Header.Roots, subject.Roots)

			for {
				gotBlock, gotErr := subject.SkipNext()
				wantBlock, wantErr := wantReader.Next()
				if wantErr != nil && gotErr == nil {
					fmt.Printf("want was %+v\n", wantReader)
					fmt.Printf("want was err, got was %+v / %d\n", gotBlock, gotBlock.Size)
				}
				require.Equal(t, wantErr, gotErr)
				if gotErr == io.EOF {
					break
				}
				if gotErr == nil {
					require.Equal(t, wantBlock.Cid(), gotBlock.Cid)
					require.Equal(t, uint64(len(wantBlock.RawData())), gotBlock.Size)
				}
			}
		})
	}
}

func TestMaxSectionLength(t *testing.T) {
	// headerHex is the zero-roots CARv1 header
	const headerHex = "11a265726f6f7473806776657273696f6e01"
	headerBytes, _ := hex.DecodeString(headerHex)
	// 8 MiB block of zeros
	block := make([]byte, 8<<20)
	// CID for that block
	pfx := cid.NewPrefixV1(cid.Raw, mh.SHA2_256)
	cid, err := pfx.Sum(block)
	require.NoError(t, err)

	// construct CAR
	var buf bytes.Buffer
	buf.Write(headerBytes)
	buf.Write(varint.ToUvarint(uint64(len(cid.Bytes()) + len(block))))
	buf.Write(cid.Bytes())
	buf.Write(block)

	// try to read it
	car, err := carv2.NewBlockReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	// error should occur on first section read
	_, err = car.Next()
	require.EqualError(t, err, "invalid section data, length of read beyond allowable maximum")

	// successful read by expanding the max section size
	car, err = carv2.NewBlockReader(bytes.NewReader(buf.Bytes()), carv2.MaxAllowedSectionSize((8<<20)+40))
	require.NoError(t, err)
	// can now read block and get our 8 MiB zeroed byte array
	readBlock, err := car.Next()
	require.NoError(t, err)
	require.True(t, bytes.Equal(block, readBlock.RawData()))
}

func TestTrustedCAR(t *testing.T) {
	// headerHex is the zero-roots CARv1 header
	const headerHex = "11a265726f6f7473806776657273696f6e01"
	headerBytes, _ := hex.DecodeString(headerHex)
	// block of zeros
	block := make([]byte, 5)
	// CID for that block
	pfx := cid.NewPrefixV1(cid.Raw, mh.SHA2_256)
	cid, err := pfx.Sum(block)
	require.NoError(t, err)

	// Modify the block so it won't match CID anymore
	block[2] = 0xFF
	// construct CAR
	var buf bytes.Buffer
	buf.Write(headerBytes)
	buf.Write(varint.ToUvarint(uint64(len(cid.Bytes()) + len(block))))
	buf.Write(cid.Bytes())
	buf.Write(block)

	// try to read it as trusted
	car, err := carv2.NewBlockReader(bytes.NewReader(buf.Bytes()), carv2.WithTrustedCAR(true))
	require.NoError(t, err)
	_, err = car.Next()
	require.NoError(t, err)

	// Try to read it as untrusted - should fail
	car, err = carv2.NewBlockReader(bytes.NewReader(buf.Bytes()), carv2.WithTrustedCAR(false))
	require.NoError(t, err)
	// error should occur on first section read
	_, err = car.Next()
	require.EqualError(t, err, "mismatch in content integrity, expected: bafkreieikviivlpbn3cxhuq6njef37ikoysaqxa2cs26zxleqxpay2bzuq, got: bafkreidgklrppelx4fxcsna7cxvo3g7ayedfojkqeuus6kz6e4hy7gukmy")
}

func TestMaxHeaderLength(t *testing.T) {
	// headerHex is the is a 5 root CARv1 header
	const headerHex = "de01a265726f6f747385d82a58250001711220785197229dc8bb1152945da58e2348f7e279eeded06cc2ca736d0e879858b501d82a58250001711220785197229dc8bb1152945da58e2348f7e279eeded06cc2ca736d0e879858b501d82a58250001711220785197229dc8bb1152945da58e2348f7e279eeded06cc2ca736d0e879858b501d82a58250001711220785197229dc8bb1152945da58e2348f7e279eeded06cc2ca736d0e879858b501d82a58250001711220785197229dc8bb1152945da58e2348f7e279eeded06cc2ca736d0e879858b5016776657273696f6e01"
	headerBytes, _ := hex.DecodeString(headerHex)
	c, _ := cid.Decode("bafyreidykglsfhoixmivffc5uwhcgshx4j465xwqntbmu43nb2dzqwfvae")

	// successful read
	car, err := carv2.NewBlockReader(bytes.NewReader(headerBytes))
	require.NoError(t, err)
	require.ElementsMatch(t, []cid.Cid{c, c, c, c, c}, car.Roots)

	// unsuccessful read, low allowable max header length (length - 3 because there are 2 bytes in the length varint prefix)
	_, err = carv2.NewBlockReader(bytes.NewReader(headerBytes), carv2.MaxAllowedHeaderSize(uint64(len(headerBytes)-3)))
	require.EqualError(t, err, "invalid header data, length of read beyond allowable maximum")
}

func TestBlockReader(t *testing.T) {
	req := require.New(t)

	// prepare a CARv1 with 100 blocks
	roots := []cid.Cid{cid.MustParse("bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk")}
	blks := make([]struct {
		block      blocks.Block
		dataOffset uint64
	}, 100)
	v1buf := new(bytes.Buffer)
	carv1.WriteHeader(&carv1.CarHeader{Roots: roots, Version: 1}, v1buf)
	vb := make([]byte, 2)
	for i := 0; i < 100; i++ {
		blk := randBlock(100 + i) // we should cross the varint two-byte boundary in here somewhere
		blks[i] = struct {
			block      blocks.Block
			dataOffset uint64
		}{block: blk, dataOffset: uint64(v1buf.Len())}
		vn := varint.PutUvarint(vb, uint64(len(blk.Cid().Bytes())+len(blk.RawData())))
		n, err := v1buf.Write(vb[:vn])
		req.NoError(err)
		req.Equal(n, vn)
		n, err = v1buf.Write(blk.Cid().Bytes())
		req.NoError(err)
		req.Equal(len(blk.Cid().Bytes()), n)
		n, err = v1buf.Write(blk.RawData())
		req.NoError(err)
		req.Equal(len(blk.RawData()), n)
	}

	v2buf := new(bytes.Buffer)
	n, err := v2buf.Write(carv2.Pragma)
	req.NoError(err)
	req.Equal(len(carv2.Pragma), n)
	v2Header := carv2.NewHeader(uint64(v1buf.Len()))
	ni, err := v2Header.WriteTo(v2buf)
	req.NoError(err)
	req.Equal(carv2.HeaderSize, int(ni))
	n, err = v2buf.Write(v1buf.Bytes())
	req.NoError(err)
	req.Equal(v1buf.Len(), n)

	v2padbuf := new(bytes.Buffer)
	n, err = v2padbuf.Write(carv2.Pragma)
	req.NoError(err)
	req.Equal(len(carv2.Pragma), n)
	v2Header = carv2.NewHeader(uint64(v1buf.Len()))
	// pad with 100 bytes
	v2Header.DataOffset += 100
	ni, err = v2Header.WriteTo(v2padbuf)
	req.NoError(err)
	req.Equal(carv2.HeaderSize, int(ni))
	v2padbuf.Write(make([]byte, 100))
	n, err = v2padbuf.Write(v1buf.Bytes())
	req.NoError(err)
	req.Equal(v1buf.Len(), n)

	for _, testCase := range []struct {
		name     string
		reader   func() io.Reader
		v1offset uint64
	}{
		{
			name:   "v1",
			reader: func() io.Reader { return &readerOnly{bytes.NewReader(v1buf.Bytes())} },
		},
		{
			name:     "v2",
			reader:   func() io.Reader { return &readerOnly{bytes.NewReader(v2buf.Bytes())} },
			v1offset: uint64(carv2.PragmaSize + carv2.HeaderSize),
		},
		{
			name:     "v2 padded",
			reader:   func() io.Reader { return &readerOnly{bytes.NewReader(v2padbuf.Bytes())} },
			v1offset: uint64(carv2.PragmaSize+carv2.HeaderSize) + 100,
		},
		{
			name:   "v1 w/ReadSeeker",
			reader: func() io.Reader { return bytes.NewReader(v1buf.Bytes()) },
		},
		{
			name:     "v2 w/ReadSeeker",
			reader:   func() io.Reader { return bytes.NewReader(v2buf.Bytes()) },
			v1offset: uint64(carv2.PragmaSize + carv2.HeaderSize),
		},
		{
			name:     "v2 padded w/ReadSeeker",
			reader:   func() io.Reader { return bytes.NewReader(v2padbuf.Bytes()) },
			v1offset: uint64(carv2.PragmaSize+carv2.HeaderSize) + 100,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			req := require.New(t)

			car, err := carv2.NewBlockReader(testCase.reader())
			req.NoError(err)
			req.ElementsMatch(roots, car.Roots)

			for i := 0; i < 100; i++ {
				blk, err := car.Next()
				req.NoError(err)
				req.Equal(blks[i].block.Cid(), blk.Cid())
				req.Equal(blks[i].block.RawData(), blk.RawData())
			}
			_, err = car.Next()
			req.ErrorIs(err, io.EOF)

			car, err = carv2.NewBlockReader(testCase.reader())
			req.NoError(err)
			req.ElementsMatch(roots, car.Roots)

			for i := 0; i < 100; i++ {
				blk, err := car.SkipNext()
				req.NoError(err)
				req.Equal(blks[i].block.Cid(), blk.Cid)
				req.Equal(uint64(len(blks[i].block.RawData())), blk.Size)
				req.Equal(blks[i].dataOffset, blk.Offset, "block #%d", i)
				req.Equal(blks[i].dataOffset+testCase.v1offset, blk.SourceOffset)
			}
			_, err = car.Next()
			req.ErrorIs(err, io.EOF)
		})
	}
}

type readerOnly struct {
	r io.Reader
}

func (r readerOnly) Read(b []byte) (int, error) {
	return r.r.Read(b)
}

func randBlock(l int) blocks.Block {
	data := make([]byte, l)
	rand.Read(data)
	h, err := mh.Sum(data, mh.SHA2_512, -1)
	if err != nil {
		panic(err)
	}
	blk, err := blocks.NewBlockWithCid(data, cid.NewCidV1(cid.Raw, h))
	if err != nil {
		panic(err)
	}
	return blk
}

func requireReaderFromPath(t *testing.T, path string) io.Reader {
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	return f
}
