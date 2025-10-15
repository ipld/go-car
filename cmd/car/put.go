package main

import (
	"context"
	"fmt"
	"io"
	"os"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

// PutCarBlock is a command to put a block into a car file
func PutCarBlock(c *cli.Context) error {
	if c.Args().Len() < 1 {
		return fmt.Errorf("usage: car put-block [options] <file.car>")
	}

	carPath := c.Args().Get(0)
	setRoot := c.Bool("set-root")

	// Read block data from stdin
	blockData, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fmt.Errorf("failed to read block data from stdin: %w", err)
	}

	// Parse codec
	codecName := c.String("codec")
	var codecCode uint64
	switch codecName {
	case "raw":
		codecCode = uint64(multicodec.Raw)
	case "dag-pb":
		codecCode = uint64(multicodec.DagPb)
	case "dag-cbor":
		codecCode = uint64(multicodec.DagCbor)
	case "dag-json":
		codecCode = uint64(multicodec.DagJson)
	default:
		return fmt.Errorf("unsupported codec: %s", codecName)
	}

	// Compute CID for the block
	pref := cid.Prefix{
		Version:  1,
		Codec:    codecCode,
		MhType:   multihash.SHA2_256,
		MhLength: -1,
	}
	blockCid, err := pref.Sum(blockData)
	if err != nil {
		return fmt.Errorf("failed to compute CID: %w", err)
	}

	blk, err := blocks.NewBlockWithCid(blockData, blockCid)
	if err != nil {
		return fmt.Errorf("failed to create block: %w", err)
	}

	// Determine roots for the CAR file
	var roots []cid.Cid
	fileExists := false

	if _, err := os.Stat(carPath); err == nil {
		fileExists = true
		if setRoot {
			return fmt.Errorf("cannot use --set-root when appending to existing file")
		}

		// Read existing roots from the file
		robs, err := blockstore.OpenReadOnly(carPath)
		if err != nil {
			return fmt.Errorf("failed to open existing CAR file: %w", err)
		}
		roots, err = robs.Roots()
		robs.Close()
		if err != nil {
			return fmt.Errorf("failed to read roots from existing CAR file: %w", err)
		}
	} else {
		// New file
		if setRoot {
			roots = []cid.Cid{blockCid}
		} else {
			roots = []cid.Cid{}
		}
	}

	// Prepare options
	var options []carv2.Option
	switch c.Int("version") {
	case 1:
		options = append(options, blockstore.WriteAsCarV1(true))
	case 2:
		// default, no option needed
	default:
		return fmt.Errorf("invalid CAR version %d", c.Int("version"))
	}

	// Open blockstore for writing
	bs, err := blockstore.OpenReadWrite(carPath, roots, options...)
	if err != nil {
		return fmt.Errorf("failed to open CAR file for writing: %w", err)
	}

	// Put the block
	ctx := context.Background()
	if err := bs.Put(ctx, blk); err != nil {
		bs.Discard()
		return fmt.Errorf("failed to put block: %w", err)
	}

	// Finalize the blockstore
	if err := bs.Finalize(); err != nil {
		return fmt.Errorf("failed to finalize CAR file: %w", err)
	}

	// Output the CID
	fmt.Println(blockCid.String())

	if fileExists {
		fmt.Fprintf(os.Stderr, "Block added to existing CAR file: %s\n", carPath)
	} else {
		fmt.Fprintf(os.Stderr, "Created new CAR file: %s\n", carPath)
	}

	return nil
}
