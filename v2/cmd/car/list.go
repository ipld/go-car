package main

import (
	"fmt"
	"io"
	"os"

	carv2 "github.com/ipld/go-car/v2"
	icarv1 "github.com/ipld/go-car/v2/internal/carv1"
	"github.com/urfave/cli/v2"
)

// ListCar is a command to output the cids in a car.
func ListCar(c *cli.Context) error {
	r, err := carv2.OpenReader(c.Args().Get(0))
	if err != nil {
		return err
	}
	defer r.Close()

	outStream := os.Stdout
	if c.Args().Len() >= 2 {
		outStream, err = os.Create(c.Args().Get(1))
		if err != nil {
			return err
		}
	}
	defer outStream.Close()

	rd, err := icarv1.NewCarReader(r.DataReader())
	if err != nil {
		return err
	}

	for {
		blk, err := rd.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		outStream.WriteString(fmt.Sprintf("%s\n", blk.Cid()))
	}

	return err
}
