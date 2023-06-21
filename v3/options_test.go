package car_test

import (
	"math"
	"testing"

	car "github.com/ipld/go-car/v3"
	"github.com/ipld/go-car/v3/blockstore"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestApplyOptions_SetsExpectedDefaults(t *testing.T) {
	require.Equal(t, car.Options{
		IndexCodec:            multicodec.CarMultihashIndexSorted,
		MaxIndexCidSize:       car.DefaultMaxIndexCidSize,
		MaxTraversalLinks:     math.MaxInt64,
		MaxAllowedHeaderSize:  32 << 20,
		MaxAllowedSectionSize: 8 << 20,
	}, car.ApplyOptions())
}

func TestApplyOptions_AppliesOptions(t *testing.T) {
	require.Equal(t,
		car.Options{
			DataPadding:                  123,
			IndexPadding:                 456,
			IndexCodec:                   multicodec.CarIndexSorted,
			ZeroLengthSectionAsEOF:       true,
			MaxIndexCidSize:              789,
			StoreIdentityCIDs:            true,
			BlockstoreAllowDuplicatePuts: true,
			BlockstoreUseWholeCIDs:       true,
			MaxTraversalLinks:            math.MaxInt64,
			MaxAllowedHeaderSize:         101,
			MaxAllowedSectionSize:        202,
		},
		car.ApplyOptions(
			car.UseDataPadding(123),
			car.UseIndexPadding(456),
			car.UseIndexCodec(multicodec.CarIndexSorted),
			car.ZeroLengthSectionAsEOF(true),
			car.MaxIndexCidSize(789),
			car.StoreIdentityCIDs(true),
			car.MaxAllowedHeaderSize(101),
			car.MaxAllowedSectionSize(202),
			blockstore.AllowDuplicatePuts(true),
			blockstore.UseWholeCIDs(true),
		))
}
