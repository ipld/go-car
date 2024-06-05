package main

import (
	"fmt"

	"github.com/ipld/go-car/cmd/car/lib"
	"github.com/urfave/cli/v2"
)

// CarRoot prints the root CID in a car
func CarRoot(c *cli.Context) (err error) {
	roots, err := lib.CarRoot(c.Args().First())
	if err != nil {
		return err
	}
	for _, r := range roots {
		fmt.Printf("%s\n", r.String())
	}

	return nil
}
