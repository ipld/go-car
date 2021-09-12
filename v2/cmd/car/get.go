package main

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/urfave/cli/v2"
)

// GetCarBlock is a command to get a block out of a car
func GetCarBlock(c *cli.Context) error {
	if c.Args().Len() < 2 {
		return fmt.Errorf("usage: car get-block <file.car> <block cid>")
	}

	bs, err := blockstore.OpenReadOnly(c.Args().Get(0))
	if err != nil {
		return err
	}

	// string to car
	blkCid, err := cid.Parse(c.Args().Get(1))
	if err != nil {
		return err
	}

	blk, err := bs.Get(blkCid)
	if err != nil {
		return err
	}
	fmt.Printf(string(blk.RawData()))
	return nil
}
