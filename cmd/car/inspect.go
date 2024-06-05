package main

import (
	"fmt"
	"os"

	"github.com/ipld/go-car/cmd/car/lib"
	"github.com/urfave/cli/v2"
)

// InspectCar verifies a CAR and prints a basic report about its contents
func InspectCar(c *cli.Context) (err error) {
	inStream := os.Stdin
	if c.Args().Len() >= 1 {
		inStream, err = os.Open(c.Args().First())
		if err != nil {
			return err
		}
	}

	rep, err := lib.InspectCar(inStream, c.Bool("full"))
	if err != nil {
		return err
	}
	fmt.Print(rep.String())
	return nil
}
