package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/file"
	"github.com/ipld/go-car/v2/blockstore"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/urfave/cli/v2"
)

// ExtractCar pulls files and directories out of a car
func ExtractCar(c *cli.Context) error {
	if !c.IsSet("file") {
		return fmt.Errorf("a file source must be specified")
	}
	outputDir, err := os.Getwd()
	if err != nil {
		return err
	}
	if c.Args().Len() > 0 {
		outputDir = c.Args().First()
	}

	if c.IsSet("verbose") {
		fmt.Printf("writing to %s\n", outputDir)
	}

	bs, err := blockstore.OpenReadOnly(c.Args().Get(0))
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
		blk, err := bs.Get(cl.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(blk.RawData()), nil
	}

	roots, err := bs.Roots()
	if err != nil {
		return err
	}

	for _, root := range roots {
		if err := extractRoot(c, &ls, root, outputDir); err != nil {
			return err
		}
	}

	return nil
}

func extractRoot(c *cli.Context, ls *ipld.LinkSystem, root cid.Cid, outputDir string) error {
	if root.Prefix().Codec == cid.Raw {
		if c.IsSet("verbose") {
			fmt.Fprintf(os.Stderr, "skipping raw root %s\n", root)
		}
		return nil
	}

	pbn, err := ls.Load(ipld.LinkContext{}, cidlink.Link{Cid: root}, dagpb.Type.PBNode)
	if err != nil {
		return err
	}
	pbnode := pbn.(dagpb.PBNode)

	ufn, err := unixfsnode.Reify(ipld.LinkContext{}, pbnode, ls)
	if err != nil {
		return err
	}

	if err := extractDir(c, ls, ufn, outputDir); err != nil {
		return fmt.Errorf("%s: %w", root, err)
	}

	return nil
}

func extractDir(c *cli.Context, ls *ipld.LinkSystem, n ipld.Node, outputDir string) error {
	// make the directory.
	os.MkdirAll(outputDir, 0755)

	if n.Kind() == ipld.Kind_Map {
		mi := n.MapIterator()
		for !mi.Done() {
			key, val, err := mi.Next()
			if err != nil {
				return err
			}
			ks, err := key.AsString()
			if err != nil {
				return err
			}
			if val.Kind() == ipld.Kind_Map {
				// interpret dagpb 'data' as unixfs data and look at type.
				ufsData, err := val.LookupByString("Data")
				if err != nil {
					return err
				}
				ufsBytes, err := ufsData.AsBytes()
				if err != nil {
					return err
				}
				ufsNode, err := data.DecodeUnixFSData(ufsBytes)
				if err != nil {
					return err
				}
				if ufsNode.DataType.Int() == data.Data_Directory || ufsNode.DataType.Int() == data.Data_HAMTShard {
					if err := extractDir(c, ls, val, path.Join(outputDir, ks)); err != nil {
						return err
					}
				} else if ufsNode.DataType.Int() == data.Data_File || ufsNode.DataType.Int() == data.Data_Raw {
					if err := extractFile(c, ls, val, path.Join(outputDir, ks)); err != nil {
						return err
					}
				} else if ufsNode.DataType.Int() == data.Data_Symlink {
					// TODO: symlink
				}
			} else {
				if err := extractFile(c, ls, val, path.Join(outputDir, ks)); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return fmt.Errorf("not a directory")
}

func extractFile(c *cli.Context, ls *ipld.LinkSystem, n ipld.Node, outputName string) error {
	node, err := file.NewUnixFSFile(c.Context, n, ls)
	if err != nil {
		return err
	}
	f, err := os.Create(outputName)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, node)

	return err
}
