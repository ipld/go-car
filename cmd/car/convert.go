package main

import (
	"bytes"
	"fmt"
	"io"
	"os"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipfsbs "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car/v2/blockstore"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorParser "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

type children struct {
	t    int
	done bool
	old  []cid.Cid
	new  []cid.Cid
}

func proxyCid(proto cidlink.LinkPrototype) (cid.Cid, error) {
	// make a cid with the right length that we eventually will patch with the root.
	hasher, err := multihash.GetHasher(proto.MhType)
	if err != nil {
		return cid.Undef, err
	}
	digest := hasher.Sum([]byte{})
	hash, err := multihash.Encode(digest, proto.MhType)
	if err != nil {
		return cid.Undef, err
	}
	proxyRoot := cid.NewCidV1(uint64(proto.Codec), hash)
	return proxyRoot, nil
}

// ConvertCar will will re-write the blocks in a car to a specified codec.
func ConvertCar(c *cli.Context) error {
	if c.Args().Len() < 3 {
		return fmt.Errorf("Usage: convert <source> <destination> <codec>")
	}

	output := c.Args().Get(1)
	bs, err := blockstore.OpenReadOnly(c.Args().Get(0))
	if err != nil {
		return err
	}
	_ = os.Remove(output)

	proto := cidlink.LinkPrototype{
		Prefix: cid.NewPrefixV1(cid.DagCBOR, multihash.SHA2_256),
	}
	p, err := proxyCid(proto)
	if err != nil {
		return err
	}
	outStore, err := blockstore.OpenReadWrite(output, []cid.Cid{p}, blockstore.AllowDuplicatePuts(false))
	if err != nil {
		return err
	}
	outls := cidlink.DefaultLinkSystem()
	outls.TrustedStorage = true
	outls.StorageWriteOpener = func(lc linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(l datamodel.Link) error {
			c := l.(cidlink.Link).Cid
			blk, _ := blocks.NewBlockWithCid(buf.Bytes(), c)
			return outStore.Put(lc.Ctx, blk)
		}, nil
	}

	roots, err := bs.Roots()
	if err != nil {
		return err
	}
	if len(roots) != 1 {
		return fmt.Errorf("car file has does not have exactly one root, dag root must be specified explicitly")
	}
	rootCid := roots[0]

	sel := selectorParser.CommonSelector_MatchAllRecursively
	if c.IsSet("selector") {
		sel, err = selectorParser.ParseJSONSelector(c.String("selector"))
		if err != nil {
			return err
		}
	}
	linkVisitOnlyOnce := !c.IsSet("selector") // if using a custom selector, this isn't as safe

	workMap := make(map[cid.Cid]*children)
	tempStore := memstore.Store{}

	// Step 1: traverse blocks into tempstore. populate workmap.
	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true
	ls.StorageReadOpener = func(_ linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		if cl, ok := l.(cidlink.Link); ok {
			blk, err := bs.Get(c.Context, cl.Cid)
			if err != nil {
				if err == ipfsbs.ErrNotFound {
					return nil, traversal.SkipMe{}
				}
				return nil, err
			}
			if err := tempStore.Put(c.Context, cl.Cid.String(), blk.RawData()); err != nil {
				return nil, err
			}
			workMap[cl.Cid] = &children{}
			return bytes.NewBuffer(blk.RawData()), nil
		}
		return nil, fmt.Errorf("unknown link type: %T", l)
	}

	nsc := func(lnk datamodel.Link, lctx ipld.LinkContext) (datamodel.NodePrototype, error) {
		if lnk, ok := lnk.(cidlink.Link); ok && lnk.Cid.Prefix().Codec == 0x70 {
			return dagpb.Type.PBNode, nil
		}
		return basicnode.Prototype.Any, nil
	}

	rootLink := cidlink.Link{Cid: rootCid}
	ns, _ := nsc(rootLink, ipld.LinkContext{})
	rootNode, err := ls.Load(ipld.LinkContext{}, rootLink, ns)
	if err != nil {
		return err
	}

	traversalProgress := traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem:                     ls,
			LinkTargetNodePrototypeChooser: nsc,
			LinkVisitOnlyOnce:              linkVisitOnlyOnce,
		},
	}

	s, err := selector.CompileSelector(sel)
	if err != nil {
		return err
	}

	err = traversalProgress.WalkAdv(rootNode, s, func(traversal.Progress, datamodel.Node, traversal.VisitReason) error { return nil })
	if err != nil {
		return err
	}

	// Step 2: traverse workmap and load blocks to get old children.
	for blkCid := range workMap {
		old := make([]cid.Cid, 0)
		lnk := cidlink.Link{Cid: blkCid}
		ns, _ = nsc(lnk, ipld.LinkContext{})
		node, err := ls.Load(ipld.LinkContext{}, lnk, ns)
		if err != nil {
			return err
		}
		traversal.WalkLocal(node, func(p traversal.Progress, n datamodel.Node) error {
			if n.Kind() == datamodel.Kind_Link {
				nlk, _ := n.AsLink()
				old = append(old, nlk.(cidlink.Link).Cid)
			}
			return nil
		})
		workMap[blkCid] = &children{t: 0, old: old, new: make([]cid.Cid, len(old))}
	}

	// Step 3: for nodes with no-uncoverted children, transform the node, and convert.
	done := 0
	xar, _ := selector.CompileSelector(selectorParser.CommonSelector_ExploreAllRecursively)
	for done < len(workMap) {
		for c := range workMap {
			if workMap[c].t == len(workMap[c].old) && !workMap[c].done {
				// Step 3.1: transform the node using old->new map
				lnk := cidlink.Link{Cid: c}
				ns, _ = nsc(lnk, ipld.LinkContext{})
				oldRoot, err := ls.Load(ipld.LinkContext{}, lnk, ns)
				if err != nil {
					return err
				}
				newRoot, err := traversal.WalkTransforming(oldRoot, xar, func(p traversal.Progress, n datamodel.Node) (datamodel.Node, error) {
					if n.Kind() == datamodel.Kind_Link {
						nlk, _ := n.AsLink()
						oldCid := nlk.(cidlink.Link).Cid
						for i, c := range workMap[c].old {
							if c.Equals(oldCid) {
								newLk := basicnode.NewLink(cidlink.Link{Cid: workMap[c].new[i]})
								return newLk, nil
							}
						}
						return nil, fmt.Errorf("could not find link %s in workmap", oldCid)
					}
					return n, nil
				})
				// Step 3.2: serialize into output datastore
				newLnk, err := outls.Store(ipld.LinkContext{}, proto, newRoot)
				if err != nil {
					return err
				}
				newCid := newLnk.(cidlink.Link).Cid

				// Step 3.3: update workmap indicating parents should transform this child.
				for d := range workMap {
					for i, o := range workMap[d].old {
						if o == newCid {
							workMap[d].new[i] = newCid
							workMap[d].t++
						}
					}
				}

				(*workMap[c]).done = true
				done++
			}
		}
	}

	return outStore.Finalize()
	// todo: fix up root cid
}
