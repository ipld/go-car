package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/cmd/car/lib"
	"github.com/urfave/cli/v2"
)

// FilterCar is a command to select a subset of a car by CID.
func FilterCar(c *cli.Context) error {
	if c.Args().Len() < 2 {
		return fmt.Errorf("an output filename must be provided")
	}

	var err error
	// Get the set of CIDs from stdin.
	inStream := os.Stdin
	if c.IsSet("cidFile") {
		inStream, err = os.Open(c.String("cidFile"))
		if err != nil {
			return err
		}
		defer inStream.Close()
	}
	cidMap, err := parseCIDS(inStream)
	if err != nil {
		return err
	}
	if c.Bool("inverse") {
		fmt.Printf("filtering out %d cids\n", len(cidMap))
	} else {
		fmt.Printf("filtering to %d cids\n", len(cidMap))
	}

	return lib.FilterCar(c.Context, c.Args().First(), c.Args().Get(1), cidMap, c.Bool("invert"), c.Int("version"), c.Bool("append"))
}

func parseCIDS(r io.Reader) (map[cid.Cid]struct{}, error) {
	cids := make(map[cid.Cid]struct{})
	br := bufio.NewReader(r)
	for {
		line, _, err := br.ReadLine()
		if err != nil {
			if err == io.EOF {
				return cids, nil
			}
			return nil, err
		}
		trimLine := strings.TrimSpace(string(line))
		if len(trimLine) == 0 {
			continue
		}
		c, err := cid.Parse(trimLine)
		if err != nil {
			return nil, err
		}
		if _, ok := cids[c]; ok {
			fmt.Fprintf(os.Stderr, "duplicate cid: %s\n", c)
		}
		cids[c] = struct{}{}
	}
}
