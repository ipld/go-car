package main

import (
	"fmt"
	"os"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/urfave/cli/v2"
)

// CarRoot prints the root CID in a car
func CarRoot(c *cli.Context) (err error) {
	if c.Args().Len() >= 1 {
		bs, err := blockstore.OpenReadOnly(c.Args().First())
		if err != nil {
			return err
		}
		roots, err := bs.Roots()
		if err != nil {
			return err
		}

		for _, r := range roots {
			fmt.Printf("%s\n", r.String())
		}
		return nil
	}

	rd, err := carv2.NewBlockReader(os.Stdin)
	if err != nil {
		return err
	}
	for _, r := range rd.Roots {
		fmt.Printf("%s\n", r.String())
	}

	return nil
}
