package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	icarv1 "github.com/ipld/go-car/v2/internal/carv1"
	"github.com/urfave/cli/v2"
)

// FilterCar is a command to select a subset of a car by CID.
func FilterCar(c *cli.Context) error {
	r, err := carv2.OpenReader(c.Args().Get(0))
	if err != nil {
		return err
	}
	defer r.Close()

	if c.Args().Len() < 2 {
		return fmt.Errorf("an output filename must be provided")
	}
	roots, err := r.Roots()
	if err != nil {
		return err
	}
	bs, err := blockstore.OpenReadWrite(c.Args().Get(1), roots)
	if err != nil {
		return err
	}

	// Get the set of CIDs from stdin.
	inStream := os.Stdin
	if c.IsSet("cidFile") {
		inStream, err = os.Open(c.String("cidFile"))
		if err != nil {
			return err
		}
		defer inStream.Close()
	}
	cidList, err := parseCIDS(inStream)
	if err != nil {
		return err
	}
	fmt.Printf("filtering to %d cids\n", len(cidList))

	cidMap := make(map[cid.Cid]struct{})
	for _, e := range cidList {
		cidMap[e] = struct{}{}
	}

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
		if _, ok := cidMap[blk.Cid()]; ok {
			if err := bs.Put(blk); err != nil {
				return err
			}
		}
	}
	return bs.Finalize()
}

func parseCIDS(r io.Reader) ([]cid.Cid, error) {
	cb := make([]cid.Cid, 0)
	br := bufio.NewReader(r)
	for {
		line, _, err := br.ReadLine()
		if err != nil {
			if err == io.EOF {
				return cb, nil
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
		cb = append(cb, c)
	}
}
