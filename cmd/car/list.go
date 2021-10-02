package main

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/dustin/go-humanize"
	data "github.com/ipfs/go-unixfsnode/data"
	carv2 "github.com/ipld/go-car/v2"
	dagpb "github.com/ipld/go-codec-dagpb"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/urfave/cli/v2"
)

// ListCar is a command to output the cids in a car.
func ListCar(c *cli.Context) error {
	inStream := os.Stdin
	var err error
	if c.Args().Len() >= 1 {
		inStream, err = os.Open(c.Args().First())
		if err != nil {
			return err
		}
		defer inStream.Close()
	}
	rd, err := carv2.NewBlockReader(inStream)
	if err != nil {
		return err
	}

	outStream := os.Stdout
	if c.Args().Len() >= 2 {
		outStream, err = os.Create(c.Args().Get(1))
		if err != nil {
			return err
		}
	}
	defer outStream.Close()

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
		if c.Bool("verbose") {
			fmt.Fprintf(outStream, "%s: %s\n",
				multicodec.Code(blk.Cid().Prefix().Codec).String(),
				blk.Cid())
			if blk.Cid().Prefix().Codec == uint64(multicodec.DagPb) {
				// parse as dag-pb
				builder := dagpb.Type.PBNode.NewBuilder()
				if err := dagpb.Unmarshal(builder, bytes.NewBuffer(blk.RawData())); err != nil {
					fmt.Fprintf(outStream, "\tnot interpretable as dag-pb: %s\n", err)
					continue
				}
				n := builder.Build()
				pbn, ok := n.(dagpb.PBNode)
				if !ok {
					continue
				}
				dl := 0
				if pbn.Data.Exists() {
					dl = len(pbn.Data.Must().Bytes())
				}
				fmt.Fprintf(outStream, "\t%d links. %d bytes\n", pbn.Links.Length(), dl)
				// example link:
				li := pbn.Links.ListIterator()
				max := 3
				for !li.Done() {
					_, l, _ := li.Next()
					max--
					pbl, ok := l.(dagpb.PBLink)
					if ok && max >= 0 {
						hsh := "<unknown>"
						lnk, ok := pbl.Hash.Link().(cidlink.Link)
						if ok {
							hsh = lnk.Cid.String()
						}
						name := "<no name>"
						if pbl.Name.Exists() {
							name = pbl.Name.Must().String()
						}
						size := 0
						if pbl.Tsize.Exists() {
							size = int(pbl.Tsize.Must().Int())
						}
						fmt.Fprintf(outStream, "\t\t%s[%s] %s\n", name, humanize.Bytes(uint64(size)), hsh)
					}
				}
				if max < 0 {
					fmt.Fprintf(outStream, "\t\t(%d total)\n", 3-max)
				}
				// see if it's unixfs.
				ufd, err := data.DecodeUnixFSData(pbn.Data.Must().Bytes())
				if err != nil {
					fmt.Fprintf(outStream, "\tnot interpretable as unixfs: %s\n", err)
					continue
				}
				fmt.Fprintf(outStream, "\tUnixfs %s\n", data.DataTypeNames[ufd.FieldDataType().Int()])
			}
		} else {
			fmt.Fprintf(outStream, "%s\n", blk.Cid())
		}
	}

	return err
}
