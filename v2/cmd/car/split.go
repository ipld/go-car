package main

import (
	"fmt"
	"io"
	"os"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/urfave/cli/v2"
)

// SplitCar is a command to output the index part of a car.
func SplitCar(c *cli.Context) error {
	r, err := carv2.OpenReader(c.Args().Get(0))
	if err != nil {
		return err
	}
	defer r.Close()

	if !r.Header.HasIndex() {
		return fmt.Errorf("No index present")
	}
	_, err = io.Copy(os.Stdout, r.IndexReader())
	return err
}
