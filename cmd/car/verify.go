package main

import (
	"fmt"

	"github.com/ipld/go-car/cmd/car/lib"
	"github.com/urfave/cli/v2"
)

// VerifyCar is a command to check a files validity
func VerifyCar(c *cli.Context) error {
	if c.Args().Len() == 0 {
		return fmt.Errorf("usage: car verify <file.car>")
	}

	return lib.VerifyCar(c.Args().First())
}
