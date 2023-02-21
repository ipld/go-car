package traversal

// Resumption is an extension to an ipld traversal Progress struct that tracks the tree of the dag as it is discovered.
// For each link, it tracks the offset that node would appear at from the beginning of the traversal, if the traversal
// were to be serialized in a car format (e.g. [size || cid || block]*, no car header offset is included)
// It can then resume the traversal based on either a path within the traversal, or a car offset.

import (
	"fmt"
	"io"
	"math"

	"github.com/ipld/go-car/v2/internal/loader"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipld/go-ipld-prime/traversal"
)

type pathNode struct {
	link     datamodel.Link
	offset   uint64
	children map[datamodel.PathSegment]*pathNode
}

func newPath(link datamodel.Link, at uint64) *pathNode {
	return &pathNode{
		link:     link,
		offset:   at,
		children: make(map[datamodel.PathSegment]*pathNode),
	}
}

func (pn pathNode) addPath(p []datamodel.PathSegment, link datamodel.Link, at uint64) {
	if len(p) == 0 {
		return
	}
	if _, ok := pn.children[p[0]]; !ok {
		child := newPath(link, at)
		pn.children[p[0]] = child
	}
	pn.children[p[0]].addPath(p[1:], link, at)
}

func (pn pathNode) allLinks() []datamodel.Link {
	if len(pn.children) == 0 {
		return []datamodel.Link{pn.link}
	}
	links := make([]datamodel.Link, 0)
	if pn.link != nil {
		links = append(links, pn.link)
	}
	for _, v := range pn.children {
		links = append(links, v.allLinks()...)
	}
	return links
}

// getPaths returns reconstructed paths in the tree rooted at 'root'
func (pn pathNode) getLinks(root datamodel.Path) []datamodel.Link {
	segs := root.Segments()
	switch len(segs) {
	case 0:
		if pn.link != nil {
			return []datamodel.Link{pn.link}
		}
		return []datamodel.Link{}
	case 1:
		// base case 1: get all paths below this child.
		next := segs[0]
		if child, ok := pn.children[next]; ok {
			return child.allLinks()
		}
		return []datamodel.Link{}
	default:
	}

	next := segs[0]
	if _, ok := pn.children[next]; !ok {
		// base case 2: not registered sub-path.
		return []datamodel.Link{}
	}
	return pn.children[next].getLinks(datamodel.NewPathNocopy(segs[1:]))
}

var errInvalid = fmt.Errorf("invalid path")

func (pn pathNode) offsetAfter(root datamodel.Path) (uint64, error) {
	// we look for offset of next sibling.
	// if no next sibling recurse up the path segments until we find a next sibling.
	segs := root.Segments()
	if len(segs) == 0 {
		return 0, errInvalid
	}
	// see if this path is a child.
	chld, ok := pn.children[segs[0]]
	if !ok {
		return 0, errInvalid
	}
	closest := chld.offset
	// try recursive path
	if len(segs) > 1 {
		co, err := chld.offsetAfter(datamodel.NewPathNocopy(segs[1:]))
		if err == nil {
			return co, err
		}
	}
	// find our next sibling
	var next uint64 = math.MaxUint64
	var nc *pathNode
	for _, v := range pn.children {
		if v.offset > closest && v.offset < next {
			next = v.offset
			nc = v
		}
	}
	if nc != nil {
		return nc.offset, nil
	}

	return 0, errInvalid
}

// TraverseResumer allows resuming a progress from a previously encountered path in the selector.
type TraverseResumer interface {
	RewindToPath(from datamodel.Path) error
	RewindToOffset(offset uint64) error
	Position() uint64
}

type traversalState struct {
	wrappedLinksystem  *linking.LinkSystem
	lsCounter          *loader.Counter
	blockNumber        int
	pathOrder          map[int]datamodel.Path
	pathTree           *pathNode
	rewindPathTarget   *datamodel.Path
	rewindOffsetTarget uint64
	pendingBlockStart  uint64 // on rewinds, we store where the counter was in order to know the length of the last read block.
	progress           *traversal.Progress
}

func (ts *traversalState) RewindToPath(from datamodel.Path) error {
	if ts.progress == nil {
		return nil
	}
	// reset progress and traverse until target.
	ts.progress.SeenLinks = make(map[datamodel.Link]struct{})
	ts.blockNumber = 0
	ts.pendingBlockStart = ts.lsCounter.Size()
	ts.lsCounter.TotalRead = 0
	ts.rewindPathTarget = &from
	return nil
}

func (ts *traversalState) RewindToOffset(offset uint64) error {
	if ts.progress == nil {
		return nil
	}
	// no-op
	if ts.lsCounter.Size() == offset {
		return nil
	}
	// reset progress and traverse until target.
	ts.progress.SeenLinks = make(map[datamodel.Link]struct{})
	ts.blockNumber = 0
	ts.pendingBlockStart = ts.lsCounter.Size()
	ts.lsCounter.TotalRead = 0
	ts.rewindOffsetTarget = offset
	return nil
}

func (ts *traversalState) Position() uint64 {
	return ts.lsCounter.Size()
}

func (ts *traversalState) traverse(lc linking.LinkContext, l ipld.Link) (io.Reader, error) {
	// when not in replay mode, we track metadata
	if ts.rewindPathTarget == nil && ts.rewindOffsetTarget == 0 {
		ts.pathOrder[ts.blockNumber] = lc.LinkPath
		ts.pathTree.addPath(lc.LinkPath.Segments(), l, ts.lsCounter.Size())
		ts.blockNumber++
		return ts.wrappedLinksystem.StorageReadOpener(lc, l)
	}

	// if we reach the target, we exit replay mode (by removing target)
	if ts.rewindPathTarget != nil && lc.LinkPath.String() == ts.rewindPathTarget.String() {
		ts.rewindPathTarget = nil
		return ts.wrappedLinksystem.StorageReadOpener(lc, l)
	}

	// if we're at the rewind offset target, we exit replay mode
	if ts.rewindOffsetTarget != 0 && ts.lsCounter.Size() >= ts.rewindOffsetTarget {
		ts.rewindOffsetTarget = 0
		return ts.wrappedLinksystem.StorageReadOpener(lc, l)
	}

	// when replaying path, we skip links not of our direct ancestor,
	// and add all links on the path under them as 'seen'
	if ts.rewindPathTarget != nil {
		targetSegments := ts.rewindPathTarget.Segments()
		seg := lc.LinkPath.Segments()
		for i, s := range seg {
			if i >= len(targetSegments) {
				break
			}
			if targetSegments[i].String() != s.String() {
				links := ts.pathTree.getLinks(datamodel.NewPathNocopy(seg[0 : i+1]))
				for _, l := range links {
					ts.progress.SeenLinks[l] = struct{}{}
				}
				var err error
				ts.lsCounter.TotalRead, err = ts.pathTree.offsetAfter(datamodel.NewPathNocopy(seg[0 : i+1]))
				if err == errInvalid {
					ts.lsCounter.TotalRead = ts.pendingBlockStart
				} else if err != nil {
					// total read is now invalid, sadly
					return nil, err
				}
				return nil, traversal.SkipMe{}
			}
		}
	}
	if ts.rewindOffsetTarget != 0 {
		links := ts.pathTree.getLinks(lc.LinkPath)
		for _, l := range links {
			ts.progress.SeenLinks[l] = struct{}{}
		}
		var err error
		ts.lsCounter.TotalRead, err = ts.pathTree.offsetAfter(lc.LinkPath)
		if err == errInvalid {
			ts.lsCounter.TotalRead = ts.pendingBlockStart
		} else if err != nil {
			return nil, err
		}
		return nil, traversal.SkipMe{}
	}

	// descend.
	return ts.wrappedLinksystem.StorageReadOpener(lc, l)
}

// WithTraversingLinksystem extends a progress for traversal such that it can
// subsequently resume and perform subsets of the walk efficiently from
// an arbitrary position within the selector traversal.
func WithTraversingLinksystem(p *traversal.Progress) (TraverseResumer, error) {
	wls, ctr := loader.CountingLinkSystem(p.Cfg.LinkSystem)
	ts := &traversalState{
		wrappedLinksystem: &wls,
		lsCounter:         ctr.(*loader.Counter),
		pathOrder:         make(map[int]datamodel.Path),
		pathTree:          newPath(nil, 0),
		progress:          p,
	}
	p.Cfg.LinkSystem.StorageReadOpener = ts.traverse
	return ts, nil
}
