package main

import (
	"io"
	"os"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

// ImportCar will take a file or stream representing a block of data
// and create a car in the specified codec such that the data is packaged
// into a single-block car file.
func ImportCar(c *cli.Context) error {
	var err error
	inStream := os.Stdin
	if c.Args().Len() >= 1 && c.Args().First() != "-" {
		inStream, err = os.Open(c.Args().First())
		if err != nil {
			return err
		}
	}
	data, err := io.ReadAll(inStream)
	if err != nil {
		return err
	}

	convertTo := multicodec.Raw
	for _, candidate := range multicodec.KnownCodes() {
		if candidate.String() == c.String("codec") {
			convertTo = candidate
		}
	}

	proto := cid.Prefix{
		Version:  1,
		Codec:    uint64(convertTo),
		MhType:   multihash.SHA2_256,
		MhLength: -1,
	}
	root, err := proto.Sum(data)
	if err != nil {
		return err
	}

	ls := cidlink.DefaultLinkSystem()
	store := memstore.Store{}
	store.Put(c.Context, string(root.KeyString()), data)
	ls.SetReadStorage(&store)

	outStream := os.Stdout
	if c.Args().Len() >= 2 {
		outStream, err = os.Create(c.Args().Get(1))
		if err != nil {
			return err
		}
		defer outStream.Close()
	}
	_, err = car.TraverseV1(c.Context, &ls, root, selectorparse.CommonSelector_MatchPoint, outStream)
	return err
}
