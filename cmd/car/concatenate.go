package main

import (
	"fmt"
	"io"
	"os"

	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/urfave/cli/v2"
)

// CatCar will concatenate the blocks from a set of source car files together into a
// combined destination car file.
// The root of the destination car will be the roots of the last specified source car.
func CatCar(c *cli.Context) error {
	var err error
	if c.Args().Len() == 0 {
		return fmt.Errorf("a least one source from must be specified")
	}

	if !c.IsSet("file") {
		return fmt.Errorf("a file destination must be specified")
	}

	options := []car.Option{}
	switch c.Int("version") {
	case 1:
		options = []car.Option{blockstore.WriteAsCarV1(true)}
	case 2:
		// already the default
	default:
		return fmt.Errorf("invalid CAR version %d", c.Int("version"))
	}

	// peak at final root
	lst := c.Args().Get(c.Args().Len() - 1)
	lstStore, err := blockstore.OpenReadOnly(lst)
	if err != nil {
		return err
	}
	roots, err := lstStore.Roots()
	if err != nil {
		return err
	}
	_ = lstStore.Close()

	cdest, err := blockstore.OpenReadWrite(c.String("file"), roots, options...)
	if err != nil {
		return err
	}

	for _, src := range c.Args().Slice() {
		f, err := os.Open(src)
		if err != nil {
			return err
		}
		blkRdr, err := car.NewBlockReader(f)
		if err != nil {
			return err
		}
		blk, err := blkRdr.Next()
		for err != io.EOF {
			if err := cdest.Put(c.Context, blk); err != nil {
				return err
			}
			blk, err = blkRdr.Next()
			if err != nil && err != io.EOF {
				return err
			}
		}

		if err := f.Close(); err != nil {
			return err
		}
	}

	return cdest.Finalize()
}
