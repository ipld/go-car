package car_test

import (
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v3"
	"github.com/stretchr/testify/require"
)

func TestEOFHandling(t *testing.T) {
	// fixture is a clean single-block, single-root CAR
	fixture, err := hex.DecodeString("3aa265726f6f747381d82a58250001711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80b6776657273696f6e012c01711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80ba165646f646779f5")
	if err != nil {
		t.Fatal(err)
	}

	load := func(t *testing.T, byts []byte) car.BlockReader {
		cr, err := car.NewBlockReader(bytes.NewReader(byts))
		if err != nil {
			t.Fatal(err)
		}

		bm, err := cr.SkipNext()
		if err != nil {
			t.Fatal(err)
		}
		if bm.Cid.String() != "bafyreiavd7u6opdcm6tqmddpnrgmvfb4enxuwglhenejmchnwqvixd5ibm" {
			t.Fatal("unexpected CID")
		}

		return cr
	}

	t.Run("CleanEOF", func(t *testing.T) {
		cr := load(t, fixture)

		c, b, err := cr.Next()
		require.ErrorIs(t, err, io.EOF)
		require.Equal(t, cid.Undef, c)
		require.Nil(t, b)
	})

	t.Run("BadVarint", func(t *testing.T) {
		fixtureBadVarint := append(fixture, 160)
		cr := load(t, fixtureBadVarint)

		c, b, err := cr.Next()
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.Equal(t, cid.Undef, c)
		require.Nil(t, b)
	})

	t.Run("TruncatedBlock", func(t *testing.T) {
		fixtureTruncatedBlock := append(fixture, 100, 0, 0)
		cr := load(t, fixtureTruncatedBlock)

		c, b, err := cr.Next()
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.Equal(t, cid.Undef, c)
		require.Nil(t, b)
	})
}

func TestBadHeaders(t *testing.T) {
	testCases := []struct {
		name   string
		hex    string
		errStr string // either the whole error string
		errPfx string // or just the prefix
	}{
		/*
			{
				"{version:2}",
				"0aa16776657273696f6e02",
				"invalid car version: 2",
				"",
			},
		*/
		{
			// an unfortunate error because we don't use a pointer
			"{roots:[baeaaaa3bmjrq]}",
			"13a165726f6f747381d82a480001000003616263",
			"invalid header: missing required fields: version",
			"",
		},
		{
			"{version:\"1\",roots:[baeaaaa3bmjrq]}",
			"1da265726f6f747381d82a4800010000036162636776657273696f6e6131",
			"", "invalid header: ",
		},
		{
			"{version:1}",
			"0aa16776657273696f6e01",
			"invalid header: no roots",
			"",
		},
		{
			"{version:1,roots:{cid:baeaaaa3bmjrq}}",
			"20a265726f6f7473a163636964d82a4800010000036162636776657273696f6e01",
			"",
			"invalid header: ",
		},
		{
			"{version:1,roots:[baeaaaa3bmjrq],blip:true}",
			"22a364626c6970f565726f6f747381d82a4800010000036162636776657273696f6e01",
			"",
			"invalid header: ",
		},
		{
			"[1,[]]",
			"03820180",
			"",
			"invalid header: ",
		},
		{
			// this is an unfortunate error, it'd be nice to catch it better but it's
			// very unlikely we'd ever see this in practice
			"null",
			"01f6",
			"",
			"invalid header: ",
		},
	}

	makeCar := func(t *testing.T, byts string) error {
		fixture, err := hex.DecodeString(byts)
		if err != nil {
			t.Fatal(err)
		}
		_, err = car.NewBlockReader(bytes.NewReader(fixture))
		return err
	}

	t.Run("Sanity check {version:1,roots:[baeaaaa3bmjrq]}", func(t *testing.T) {
		err := makeCar(t, "1ca265726f6f747381d82a4800010000036162636776657273696f6e01")
		if err != nil {
			t.Fatal(err)
		}
	})

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := makeCar(t, tc.hex)
			require.Error(t, err)
			if tc.errStr != "" {
				require.Equal(t, tc.errStr, err.Error())
			} else {
				require.Truef(t, strings.HasPrefix(err.Error(), tc.errPfx), "bad error: %s", err.Error())
			}
		})
	}
}

func TestV1HeaderMatches(t *testing.T) {
	oneCid := blocks.NewBlock([]byte("fish")).Cid()
	anotherCid := blocks.NewBlock([]byte("lobster")).Cid()
	tests := []struct {
		name  string
		one   car.V1Header
		other car.V1Header
		want  bool
	}{
		{
			"SameVersionNilRootsIsMatching",
			car.V1Header{nil, 1},
			car.V1Header{nil, 1},
			true,
		},
		{
			"SameVersionEmptyRootsIsMatching",
			car.V1Header{[]cid.Cid{}, 1},
			car.V1Header{[]cid.Cid{}, 1},
			true,
		},
		{
			"SameVersionNonEmptySameRootsIsMatching",
			car.V1Header{[]cid.Cid{oneCid}, 1},
			car.V1Header{[]cid.Cid{oneCid}, 1},
			true,
		},
		{
			"SameVersionNonEmptySameRootsInDifferentOrderIsMatching",
			car.V1Header{[]cid.Cid{oneCid, anotherCid}, 1},
			car.V1Header{[]cid.Cid{anotherCid, oneCid}, 1},
			true,
		},
		{
			"SameVersionDifferentRootsIsNotMatching",
			car.V1Header{[]cid.Cid{oneCid}, 1},
			car.V1Header{[]cid.Cid{anotherCid}, 1},
			false,
		},
		{
			"DifferentVersionDifferentRootsIsNotMatching",
			car.V1Header{[]cid.Cid{oneCid}, 0},
			car.V1Header{[]cid.Cid{anotherCid}, 1},
			false,
		},
		{
			"MismatchingVersionIsNotMatching",
			car.V1Header{nil, 0},
			car.V1Header{nil, 1},
			false,
		},
		{
			"ZeroValueHeadersAreMatching",
			car.V1Header{},
			car.V1Header{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.one.Matches(tt.other)
			require.Equal(t, tt.want, got, "Matches() = %v, want %v", got, tt.want)
		})
	}
}

func TestReadingZeroLengthSectionWithoutOptionSetIsError(t *testing.T) {
	f, err := os.Open("testdata/sample-v1-with-zero-len-section.car")
	require.NoError(t, err)
	subject, err := car.NewBlockReader(f)
	require.NoError(t, err)

	for {
		_, _, err := subject.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			require.EqualError(t, err, "invalid cid: varints malformed, could not reach the end")
			return
		}
	}
	require.Fail(t, "expected error when reading file with zero section without option set")
}

func TestReadingZeroLengthSectionWithOptionSetIsSuccess(t *testing.T) {
	f, err := os.Open("testdata/sample-v1-with-zero-len-section.car")
	require.NoError(t, err)
	subject, err := car.NewBlockReader(f, car.ZeroLengthSectionAsEOF(true))
	require.NoError(t, err)

	for {
		_, _, err := subject.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
}
