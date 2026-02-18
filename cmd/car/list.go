package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	data "github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/hamt"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/urfave/cli/v2"
)

// ListCar is a command to output the cids in a car.
func ListCar(c *cli.Context) error {
	var err error
	outStream := os.Stdout
	if c.Args().Len() >= 2 {
		outStream, err = os.Create(c.Args().Get(1))
		if err != nil {
			return err
		}
	}
	defer outStream.Close()

	if c.Bool("unixfs") {
		return listUnixfs(c, outStream)
	}

	inStream := os.Stdin
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

	for {
		blk, err := rd.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if c.Bool("verbose") {
			fmt.Fprintf(outStream, "%s", multicodec.Code(blk.Cid().Prefix().Codec).String())
			if c.Bool("cids") {
				fmt.Fprintf(outStream, ": %s", blk.Cid())
			}
			fmt.Fprintln(outStream)

			if blk.Cid().Prefix().Codec == uint64(multicodec.DagPb) {
				// parse as dag-pb
				builder := dagpb.Type.PBNode.NewBuilder()
				if err := dagpb.DecodeBytes(builder, blk.RawData()); err != nil {
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
						name := "<no name>"
						if pbl.Name.Exists() {
							name = pbl.Name.Must().String()
						}
						fmt.Fprintf(outStream, "\t\t%s", name)

						size := uint64(0)
						if pbl.Tsize.Exists() {
							size = uint64(pbl.Tsize.Must().Int())
						}
						sizePart := sizeStr(c, size)
						if sizePart != "" {
							fmt.Fprintf(outStream, "%s", sizePart)
						}

						if c.Bool("cids") {
							hsh := "<unknown>"

							lnk, ok := pbl.Hash.Link().(cidlink.Link)
							if ok {
								hsh = lnk.Cid.String()
							}

							fmt.Fprintf(outStream, " %s", hsh)
						}

						fmt.Fprintln(outStream)
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
			printEntry(c, blk.Cid(), "", uint64(len(blk.RawData())), outStream)
		}
	}

	return err
}

func listUnixfs(c *cli.Context, outStream io.Writer) error {
	if c.Args().Len() == 0 {
		return fmt.Errorf("must provide file to read from. unixfs reading requires random access")
	}

	bs, err := blockstore.OpenReadOnly(c.Args().First())
	if err != nil {
		return err
	}
	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true
	ls.StorageReadOpener = func(_ ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		cl, ok := l.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("not a cidlink")
		}
		blk, err := bs.Get(c.Context, cl.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(blk.RawData()), nil
	}

	roots, err := bs.Roots()
	if err != nil {
		return err
	}
	for _, r := range roots {
		if _, err := printUnixFSNode(c, "", r, &ls, outStream); err != nil {
			return err
		}
	}
	return nil
}

func printUnixFSNode(c *cli.Context, prefix string, node cid.Cid, ls *ipld.LinkSystem, outStream io.Writer) (uint64, error) {
	if node.Prefix().Codec == cid.Raw {
		link := cidlink.Link{Cid: node}

		rawBytes, err := ls.LoadRaw(
			ipld.LinkContext{Ctx: c.Context},
			link,
		)

		if err != nil {
			return 0, err
		}

		return uint64(len(rawBytes)), nil
	}

	pbn, err := ls.Load(ipld.LinkContext{}, cidlink.Link{Cid: node}, dagpb.Type.PBNode)
	if err != nil {
		return 0, err
	}

	pbnode := pbn.(dagpb.PBNode)

	ufd, err := data.DecodeUnixFSData(pbnode.Data.Must().Bytes())
	if err != nil {
		return 0, err
	}

	var totalSize uint64 = 0

	switch ufd.FieldDataType().Int() {

	case data.Data_Directory:
		i := pbnode.Links.Iterator()
		for !i.Done() {
			_, link := i.Next()
			name := link.Name.Must().String()
			if name == "" {
				continue
			}

			chPath := path.Join(prefix, name)

			chLink, err := link.Hash.AsLink()
			if err != nil {
				return 0, err
			}

			chCid := chLink.(cidlink.Link).Cid

			chSize, err := printUnixFSNode(c, chPath, chCid, ls, outStream)
			if err != nil {
				return 0, err
			}

			totalSize += chSize

			printEntry(c, chCid, chPath, chSize, outStream)
		}

	case data.Data_HAMTShard:
		hn, err := hamt.AttemptHAMTShardFromNode(c.Context, pbn, ls)
		if err != nil {
			return 0, err
		}

		i := hn.Iterator()
		for !i.Done() {
			key, val := i.Next()

			chPath := path.Join(prefix, key.String())

			chLink, err := val.AsLink()
			if err != nil {
				return 0, err
			}

			chCid := chLink.(cidlink.Link).Cid

			chSize, err := printUnixFSNode(c, chPath, chCid, ls, outStream)
			if err != nil {
				return 0, err
			}

			totalSize += chSize

			printEntry(c, chCid, chPath, chSize, outStream)
		}

	case data.Data_File:
		size := uint64(0)
		if ufd.FieldFileSize().Exists() {
			size = uint64(ufd.FieldFileSize().Must().Int())
		}

		return size, nil

	case data.Data_Raw:
		size := uint64(0)
		if ufd.FieldData().Exists() {
			size = uint64(len(ufd.FieldData().Must().Bytes()))
		}

		return size, nil

	default:
		return 0, nil
	}

	return totalSize, nil
}

func printEntry(c *cli.Context, cid cid.Cid, path string, size uint64, outStream io.Writer) {
	parts := make([]string, 0, 3)

	if path != "" {
		// For unixfs only show CIDs if explicitly requested
		if c.IsSet("cids") && c.Bool("cids") {
			parts = append(parts, cid.String())
		}

		parts = append(parts, path)
	} else if c.Bool("cids") {
		parts = append(parts, cid.String())
	}

	sizePart := sizeStr(c, size)
	if sizePart != "" {
		parts = append(parts, sizePart)
	}

	if len(parts) == 0 {
		return
	}

	fmt.Fprintln(outStream, strings.Join(parts, " "))
}

func sizeStr(c *cli.Context, size uint64) string {
	mode := ""

	if c.IsSet("sizes") {
		mode = c.String("sizes")
	} else if c.Bool("verbose") {
		mode = "human"
	}

	switch mode {
	case "human":
		return fmt.Sprintf("[%s]", humanize.Bytes(uint64(size)))
	case "bytes":
		return fmt.Sprintf("[%d]", size)
	}

	return ""
}
