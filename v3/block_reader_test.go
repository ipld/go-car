package car_test

import (
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	car "github.com/ipld/go-car/v3"
	"github.com/ipld/go-car/v3/internal/testutil"
	mh "github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
	"github.com/stretchr/testify/require"
)

func TestBlockReaderFailsOnUnknownVersion(t *testing.T) {
	r := requireReaderFromPath(t, "testdata/sample-rootless-v42.car")
	_, err := car.NewBlockReader(r)
	require.EqualError(t, err, "invalid car version: 42")
}

func TestBlockReaderFailsOnCorruptPragma(t *testing.T) {
	r := requireReaderFromPath(t, "testdata/sample-corrupt-pragma.car")
	_, err := car.NewBlockReader(r)
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
			subject, err := car.NewBlockReader(r, car.ZeroLengthSectionAsEOF(tt.zerLenAsEOF))
			require.NoError(t, err)

			require.Equal(t, tt.wantVersion, subject.Version())

			var wantReader car.BlockReader
			switch tt.wantVersion {
			case 1:
				wantReader = requireNewCarV1ReaderFromV1File(t, tt.path, tt.zerLenAsEOF)
			case 2:
				wantReader = requireNewCarV1ReaderFromV2File(t, tt.path, tt.zerLenAsEOF)
			default:
				require.Failf(t, "invalid test-case", "unknown wantVersion %v", tt.wantVersion)
			}
			require.Equal(t, wantReader.Roots(), subject.Roots())

			for {
				wantCid, wantBytes, wantErr := wantReader.Next()
				gotCid, gotBytes, gotErr := subject.Next()
				require.Equal(t, wantCid, gotCid)
				require.Equal(t, wantBytes, gotBytes)
				require.Equal(t, wantErr, gotErr)
				if gotErr == io.EOF {
					break
				}
			}
		})
		t.Run(tt.name+"-skipping-reads", func(t *testing.T) {
			r := requireReaderFromPath(t, tt.path)
			subject, err := car.NewBlockReader(r, car.ZeroLengthSectionAsEOF(tt.zerLenAsEOF))
			require.NoError(t, err)

			require.Equal(t, tt.wantVersion, subject.Version())

			var wantReader car.BlockReader
			switch tt.wantVersion {
			case 1:
				wantReader = requireNewCarV1ReaderFromV1File(t, tt.path, tt.zerLenAsEOF)
			case 2:
				wantReader = requireNewCarV1ReaderFromV2File(t, tt.path, tt.zerLenAsEOF)
			default:
				require.Failf(t, "invalid test-case", "unknown wantVersion %v", tt.wantVersion)
			}
			require.Equal(t, wantReader.Roots(), subject.Roots())

			for {
				wantCid, wantBytes, wantErr := wantReader.Next()
				gotCid, gotBytes, gotErr := subject.Next()
				require.Equal(t, wantCid, gotCid)
				require.Equal(t, wantBytes, gotBytes)
				require.Equal(t, wantErr, gotErr)
				if wantErr != nil && gotErr == nil {
					t.Logf("want was %+v\n", wantReader)
				}
				require.Equal(t, wantErr, gotErr)
				if gotErr == io.EOF {
					break
				}
				if gotErr == nil {
					require.Equal(t, wantCid, gotCid)
					require.Len(t, gotBytes, len(wantBytes))
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
	rdr, err := car.NewBlockReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	// error should occur on first section read
	_, _, err = rdr.Next()
	require.EqualError(t, err, "invalid section data, length of read beyond allowable maximum")

	// successful read by expanding the max section size
	rdr, err = car.NewBlockReader(bytes.NewReader(buf.Bytes()), car.MaxAllowedSectionSize((8<<20)+40))
	require.NoError(t, err)
	// can now read block and get our 8 MiB zeroed byte array
	_, byts, err := rdr.Next()
	require.NoError(t, err)
	require.True(t, bytes.Equal(block, byts))
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
	rdr, err := car.NewBlockReader(bytes.NewReader(buf.Bytes()), car.WithTrustedCAR(true))
	require.NoError(t, err)
	_, _, err = rdr.Next()
	require.NoError(t, err)

	// Try to read it as untrusted - should fail
	rdr, err = car.NewBlockReader(bytes.NewReader(buf.Bytes()), car.WithTrustedCAR(false))
	require.NoError(t, err)
	// error should occur on first section read
	_, _, err = rdr.Next()
	require.EqualError(t, err, "mismatch in content integrity, expected: bafkreieikviivlpbn3cxhuq6njef37ikoysaqxa2cs26zxleqxpay2bzuq, got: bafkreidgklrppelx4fxcsna7cxvo3g7ayedfojkqeuus6kz6e4hy7gukmy")
}

func TestMaxHeaderLength(t *testing.T) {
	// headerHex is the is a 5 root CARv1 header
	const headerHex = "de01a265726f6f747385d82a58250001711220785197229dc8bb1152945da58e2348f7e279eeded06cc2ca736d0e879858b501d82a58250001711220785197229dc8bb1152945da58e2348f7e279eeded06cc2ca736d0e879858b501d82a58250001711220785197229dc8bb1152945da58e2348f7e279eeded06cc2ca736d0e879858b501d82a58250001711220785197229dc8bb1152945da58e2348f7e279eeded06cc2ca736d0e879858b501d82a58250001711220785197229dc8bb1152945da58e2348f7e279eeded06cc2ca736d0e879858b5016776657273696f6e01"
	headerBytes, _ := hex.DecodeString(headerHex)
	c, _ := cid.Decode("bafyreidykglsfhoixmivffc5uwhcgshx4j465xwqntbmu43nb2dzqwfvae")

	// successful read
	rdr, err := car.NewBlockReader(bytes.NewReader(headerBytes))
	require.NoError(t, err)
	require.ElementsMatch(t, []cid.Cid{c, c, c, c, c}, rdr.Roots())

	// unsuccessful read, low allowable max header length (length - 3 because there are 2 bytes in the length varint prefix)
	_, err = car.NewBlockReader(bytes.NewReader(headerBytes), car.MaxAllowedHeaderSize(uint64(len(headerBytes)-3)))
	require.EqualError(t, err, "invalid header data, length of read beyond allowable maximum")
}

func requireReaderFromPath(t *testing.T, path string) io.Reader {
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	return f
}

func requireNewCarV1ReaderFromV1File(t *testing.T, carV1Path string, zeroLenAsEOF bool) car.BlockReader {
	f, err := os.Open(carV1Path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	rdr, err := testutil.NewV1Reader(f, zeroLenAsEOF, 32<<20, 8<<20)
	require.NoError(t, err)
	return rdr
}

func requireNewCarV1ReaderFromV2File(t *testing.T, carV2Path string, zeroLenAsEOF bool) car.BlockReader {
	f, err := os.Open(carV2Path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	rdr, err := testutil.NewV1ReaderFromV2(f, zeroLenAsEOF, 32<<20, 8<<20)
	require.NoError(t, err)
	return rdr
}
