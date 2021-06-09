package car_test

import (
	"bytes"
	cbor "github.com/ipfs/go-ipld-cbor"
	car_v1 "github.com/ipld/go-car"
	car_v2 "github.com/ipld/go-car/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCarV2PrefixLength(t *testing.T) {
	tests := []struct {
		name string
		want interface{}
		got  interface{}
	}{
		{
			"cached length should be 11 bytes",
			11,
			car_v2.PrefixBytesLen,
		},
		{
			"actual length should be 11 bytes",
			len(car_v2.PrefixBytes),
			car_v2.PrefixBytesLen,
		},
		{
			"should start with varint(10)",
			car_v2.PrefixBytes[0],
			10,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			assert.EqualValues(t, tt.want, tt.got, "CarV2Prefix got = %v, want %v", tt.got, tt.want)
		})
	}
}

func TestCarV2PrefixIsValidCarV1Header(t *testing.T) {
	var v1h car_v1.CarHeader
	err := cbor.DecodeInto(car_v2.PrefixBytes[1:], &v1h)
	assert.NoError(t, err, "cannot decode prefix as CBOR with CAR v1 header structure")
	assert.Equal(t, car_v1.CarHeader{
		Roots:   nil,
		Version: 2,
	}, v1h, "CAR v2 prefix must be a valid CAR v1 header")
}

func TestEmptyCharacteristics(t *testing.T) {
	tests := []struct {
		name string
		want interface{}
		got  interface{}
	}{
		{
			"is of length 16 bytes",
			16,
			len(car_v2.EmptyCharacteristics),
		},
		{
			"is a whole lot of nothin'",
			[]byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
			car_v2.EmptyCharacteristics,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			assert.EqualValues(t, tt.want, tt.got, "EmptyCharacteristics got = %v, want %v", tt.got, tt.want)
		})
	}
}

func TestHeader_Marshal(t *testing.T) {
	tests := []struct {
		name        string
		target      car_v2.Header
		wantMarshal []byte
		wantErr     bool
	}{
		{
			"header with nil characteristics is marshalled as empty characteristics",
			car_v2.Header{
				Characteristics: nil,
			},
			[]byte{
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			},
			false,
		},
		{
			"header with empty characteristics is marshalled as expected",
			car_v2.Header{
				Characteristics: car_v2.EmptyCharacteristics,
			},
			[]byte{
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			},
			false,
		},
		{
			"non-empty header is marshalled as expected",
			car_v2.Header{
				Characteristics: car_v2.Characteristics{
					0x0, 0x0a, 0x09, 0x02, 0x3, 0x0, 0x0, 0x0c,
					0x0, 0x0a, 0x8, 0x01, 0x3, 0x0, 0x2, 0x0,
				},
				CarV1Len:    100,
				IndexOffset: 101,
			},
			[]byte{
				0x0, 0x0a, 0x09, 0x02, 0x3, 0x0, 0x0, 0x0c,
				0x0, 0x0a, 0x8, 0x01, 0x3, 0x0, 0x2, 0x0,
				0x64, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x65, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := tt.target.Marshal(buf)
			if (err != nil) != tt.wantErr {
				t.Errorf("Marshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			gotMarshal := buf.Bytes()
			assert.Equal(t, tt.wantMarshal, gotMarshal, "Header.Marshal() gotMarshal = %v, wantMarshal %v", gotMarshal, tt.wantMarshal)
			assert.Equal(t, car_v2.HeaderBytesLen, len(gotMarshal), "marshalled CAR v2 header must always be %v bytes long", car_v2.HeaderBytesLen)
		})
	}
}

func TestHeader_WithPadding(t *testing.T) {
	wantCarV1Len := uint64(1413)
	wantPadding := uint64(7)
	// Index is added at the end of CAR v2, after the dump of CAR v1.
	// Since the prefix and CAR v2 header are of fixed size, the offset of the index from the
	// beginning of a CAR v2 array of bytes should be the sum of all sizes, plus the padding:
	wantIndexOffset := car_v2.PrefixBytesLen + car_v2.HeaderBytesLen + wantCarV1Len + wantPadding
	want := &car_v2.Header{
		Characteristics: car_v2.EmptyCharacteristics,
		CarV1Len:        wantCarV1Len,
		IndexOffset:     wantIndexOffset,
	}
	got := car_v2.NewHeader(wantCarV1Len).WithPadding(wantPadding)
	assert.Equal(t, want, got, "NewHeader().WithPadding got = %v, want = %v", got, want)
}

func TestNewHeaderHasExpectedValues(t *testing.T) {
	wantCarV1Len := uint64(1413)
	want := &car_v2.Header{
		Characteristics: car_v2.EmptyCharacteristics,
		CarV1Len:        wantCarV1Len,
		IndexOffset:     0,
	}
	got := car_v2.NewHeader(wantCarV1Len)
	assert.Equal(t, want, got, "NewHeader got = %v, want = %v", got, want)
}
