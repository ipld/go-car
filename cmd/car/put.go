package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
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
	cidStr := c.String("cid")
	codecName := c.String("codec")

	if cidStr != "" && codecName != "raw" {
		return fmt.Errorf("--cid and --codec are mutually exclusive (use one or the other)")
	}

	blockData, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fmt.Errorf("failed to read block data from stdin: %w", err)
	}

	var blockCid cid.Cid
	var blk blocks.Block

	if cidStr != "" {
		blockCid, err = cid.Parse(cidStr)
		if err != nil {
			return fmt.Errorf("failed to parse CID: %w", err)
		}

		computedCid, err := blockCid.Prefix().Sum(blockData)
		if err != nil {
			return fmt.Errorf("failed to compute CID: %w", err)
		}

		if !blockCid.Equals(computedCid) {
			return fmt.Errorf("CID mismatch: expected %s, computed %s", blockCid, computedCid)
		}

		blk, err = blocks.NewBlockWithCid(blockData, blockCid)
		if err != nil {
			return fmt.Errorf("failed to create block: %w", err)
		}
	} else {
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

		// Currently only supporting CIDv1 with sha2-256 multihash
		pref := cid.Prefix{
			Version:  1,
			Codec:    codecCode,
			MhType:   multihash.SHA2_256,
			MhLength: -1,
		}
		blockCid, err = pref.Sum(blockData)
		if err != nil {
			return fmt.Errorf("failed to compute CID: %w", err)
		}

		blk, err = blocks.NewBlockWithCid(blockData, blockCid)
		if err != nil {
			return fmt.Errorf("failed to create block: %w", err)
		}
	}

	// Get version
	version := c.Int("version")
	if version != 1 && version != 2 {
		return fmt.Errorf("invalid CAR version %d", version)
	}

	// Check if file exists
	fileExists := false
	if _, err := os.Stat(carPath); err == nil {
		fileExists = true
		if setRoot {
			return fmt.Errorf("cannot use --set-root when appending to existing file")
		}
	}

	// For CARv1, use naive concatenation approach
	if version == 1 {
		return putBlockV1(carPath, blk, blockCid, setRoot, fileExists)
	}

	// For CARv2, use the blockstore API
	return putBlockV2(carPath, blk, blockCid, setRoot, fileExists)
}

// putBlockV1 appends a block to a CARv1 file using naive concatenation.
func putBlockV1(carPath string, blk blocks.Block, blockCid cid.Cid, setRoot, fileExists bool) error {
	if fileExists {
		f, err := os.OpenFile(carPath, os.O_RDWR, 0o666)
		if err != nil {
			return fmt.Errorf("failed to open existing CAR file: %w", err)
		}
		defer f.Close()

		br := bufio.NewReader(f)
		header, err := car.ReadHeader(br)
		if err != nil {
			return fmt.Errorf("failed to read CAR header: %w", err)
		}

		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			return fmt.Errorf("failed to seek to end of file: %w", err)
		}

		if err := util.LdWrite(f, blockCid.Bytes(), blk.RawData()); err != nil {
			return fmt.Errorf("failed to write block: %w", err)
		}

		fmt.Println(blockCid.String())
		fmt.Fprintf(os.Stderr, "Block added to existing CAR file: %s (roots: %d)\n", carPath, len(header.Roots))
		return nil
	}

	f, err := os.Create(carPath)
	if err != nil {
		return fmt.Errorf("failed to create CAR file: %w", err)
	}
	defer f.Close()

	var roots []cid.Cid
	if setRoot {
		roots = []cid.Cid{blockCid}
	} else {
		roots = []cid.Cid{}
	}

	header := &car.CarHeader{
		Roots:   roots,
		Version: 1,
	}
	if err := car.WriteHeader(header, f); err != nil {
		return fmt.Errorf("failed to write CAR header: %w", err)
	}

	if err := util.LdWrite(f, blockCid.Bytes(), blk.RawData()); err != nil {
		return fmt.Errorf("failed to write block: %w", err)
	}

	fmt.Println(blockCid.String())
	fmt.Fprintf(os.Stderr, "Created new CAR file: %s\n", carPath)
	return nil
}

func putBlockV2(carPath string, blk blocks.Block, blockCid cid.Cid, setRoot, fileExists bool) error {
	var roots []cid.Cid

	if fileExists {
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
		if setRoot {
			roots = []cid.Cid{blockCid}
		} else {
			roots = []cid.Cid{}
		}
	}

	bs, err := blockstore.OpenReadWrite(carPath, roots)
	if err != nil {
		return fmt.Errorf("failed to open CAR file for writing: %w", err)
	}

	ctx := context.Background()
	if err := bs.Put(ctx, blk); err != nil {
		bs.Discard()
		return fmt.Errorf("failed to put block: %w", err)
	}

	if err := bs.Finalize(); err != nil {
		return fmt.Errorf("failed to finalize CAR file: %w", err)
	}

	fmt.Println(blockCid.String())

	if fileExists {
		fmt.Fprintf(os.Stderr, "Block added to existing CAR file: %s\n", carPath)
	} else {
		fmt.Fprintf(os.Stderr, "Created new CAR file: %s\n", carPath)
	}

	return nil
}
