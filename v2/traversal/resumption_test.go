package traversal_test

import (
	"errors"
	"testing"

	cartraversal "github.com/ipld/go-car/v2/traversal"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
)

var store = memstore.Store{}
var (
	// baguqeeyexkjwnfy
	_, leafAlphaLnk = encode(basicnode.NewString("alpha"))
	// baguqeeyeqvc7t3a
	_, leafBetaLnk = encode(basicnode.NewString("beta"))
	// baguqeeyezhlahvq
	_, middleMapNodeLnk = encode(fluent.MustBuildMap(basicnode.Prototype.Map, 3, func(na fluent.MapAssembler) {
		na.AssembleEntry("foo").AssignBool(true)
		na.AssembleEntry("bar").AssignBool(false)
		na.AssembleEntry("nested").CreateMap(2, func(na fluent.MapAssembler) {
			na.AssembleEntry("alink").AssignLink(leafAlphaLnk)
			na.AssembleEntry("nonlink").AssignString("zoo")
		})
	}))
	// baguqeeyehfkkfwa
	_, middleListNodeLnk = encode(fluent.MustBuildList(basicnode.Prototype.List, 4, func(na fluent.ListAssembler) {
		na.AssembleValue().AssignLink(leafAlphaLnk)
		na.AssembleValue().AssignLink(leafAlphaLnk)
		na.AssembleValue().AssignLink(leafBetaLnk)
		na.AssembleValue().AssignLink(leafAlphaLnk)
	}))
	// note that using `rootNode` directly will have a different field ordering than
	// the encoded form if you were to load `rootNodeLnk` due to dag-json field
	// reordering on encode, beware the difference for traversal order between
	// created, in-memory nodes and those that have passed through a codec with
	// field ordering rules
	// baguqeeyeie4ajfy
	rootNode, _ = encode(fluent.MustBuildMap(basicnode.Prototype.Map, 4, func(na fluent.MapAssembler) {
		na.AssembleEntry("plain").AssignString("olde string")
		na.AssembleEntry("linkedString").AssignLink(leafAlphaLnk)
		na.AssembleEntry("linkedMap").AssignLink(middleMapNodeLnk)
		na.AssembleEntry("linkedList").AssignLink(middleListNodeLnk)
	}))
)

// encode hardcodes some encoding choices for ease of use in fixture generation;
// just gimme a link and stuff the bytes in a map.
// (also return the node again for convenient assignment.)
func encode(n datamodel.Node) (datamodel.Node, datamodel.Link) {
	lp := cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1,
		Codec:    0x0129,
		MhType:   0x13,
		MhLength: 4,
	}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetWriteStorage(&store)

	lnk, err := lsys.Store(linking.LinkContext{}, lp, n)
	if err != nil {
		panic(err)
	}
	return n, lnk
}

func TestWalkResumeByPath(t *testing.T) {
	seen := 0
	count := func(p traversal.Progress, n datamodel.Node, _ traversal.VisitReason) error {
		seen++
		return nil
	}

	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(&store)
	p := traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem:                     lsys,
			LinkTargetNodePrototypeChooser: basicnode.Chooser,
		},
	}
	resumer, err := cartraversal.WithTraversingLinksystem(&p, cartraversal.NewPathState())
	if err != nil {
		t.Fatal(err)
	}
	sd := selectorparse.CommonSelector_ExploreAllRecursively
	s, _ := selector.CompileSelector(sd)
	if err := p.WalkAdv(rootNode, s, count); err != nil {
		t.Fatal(err)
	}
	if seen != 14 {
		t.Fatalf("expected total traversal to visit 14 nodes, got %d", seen)
	}

	// resume from beginning.
	resumer.RewindToPath(datamodel.NewPath(nil))
	seen = 0
	if err := p.WalkAdv(rootNode, s, count); err != nil {
		t.Fatal(err)
	}
	if seen != 14 {
		t.Fatalf("expected resumed traversal to visit 14 nodes, got %d", seen)
	}

	// resume from middle.
	resumer.RewindToPath(datamodel.NewPath([]datamodel.PathSegment{datamodel.PathSegmentOfString("linkedMap")}))
	seen = 0
	if err := p.WalkAdv(rootNode, s, count); err != nil {
		t.Fatal(err)
	}
	// one less: will not visit 'linkedString' before linked map.
	if seen != 13 {
		t.Fatalf("expected resumed traversal to visit 13 nodes, got %d", seen)
	}

	// resume from middle.
	resumer.RewindToPath(datamodel.NewPath([]datamodel.PathSegment{datamodel.PathSegmentOfString("linkedList")}))
	seen = 0
	if err := p.WalkAdv(rootNode, s, count); err != nil {
		t.Fatal(err)
	}
	// will not visit 'linkedString' or 'linkedMap' before linked list.
	if seen != 7 {
		t.Fatalf("expected resumed traversal to visit 7 nodes, got %d", seen)
	}
}

func TestWalkResumeByPathPartialWalk(t *testing.T) {
	seen := 0
	limit := 0
	countUntil := func(p traversal.Progress, n datamodel.Node, _ traversal.VisitReason) error {
		seen++
		if seen >= limit {
			return traversal.SkipMe{}
		}
		return nil
	}

	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(&store)
	p := traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem:                     lsys,
			LinkTargetNodePrototypeChooser: basicnode.Chooser,
		},
	}
	resumer, err := cartraversal.WithTraversingLinksystem(&p, cartraversal.NewPathState())
	if err != nil {
		t.Fatal(err)
	}
	sd := selectorparse.CommonSelector_ExploreAllRecursively
	s, _ := selector.CompileSelector(sd)
	limit = 9
	if err := p.WalkAdv(rootNode, s, countUntil); !errors.Is(err, traversal.SkipMe{}) {
		t.Fatal(err)
	}
	if seen != limit {
		t.Fatalf("expected partial traversal, got %d", seen)
	}

	// resume.
	resumer.RewindToPath(datamodel.NewPath([]datamodel.PathSegment{datamodel.PathSegmentOfString("linkedMap")}))
	seen = 0
	limit = 14
	if err := p.WalkAdv(rootNode, s, countUntil); err != nil {
		t.Fatal(err)
	}
	if seen != 13 {
		t.Fatalf("expected resumed traversal to visit 13 nodes, got %d", seen)
	}
}

func TestWalkResumeByOffset(t *testing.T) {
	seen := 0
	count := func(p traversal.Progress, n datamodel.Node, r traversal.VisitReason) error {
		seen++
		return nil
	}

	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(&store)
	p := traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem:                     lsys,
			LinkTargetNodePrototypeChooser: basicnode.Chooser,
		},
	}
	resumer, err := cartraversal.WithTraversingLinksystem(&p, cartraversal.NewPathState())
	if err != nil {
		t.Fatal(err)
	}
	sd := selectorparse.CommonSelector_ExploreAllRecursively
	s, _ := selector.CompileSelector(sd)
	if err := p.WalkAdv(rootNode, s, count); err != nil {
		t.Fatal(err)
	}
	if seen != 14 {
		t.Fatalf("expected total traversal to visit 14 nodes, got %d", seen)
	}

	// resume from beginning.
	resumer.RewindToOffset(0)
	seen = 0
	if err := p.WalkAdv(rootNode, s, count); err != nil {
		t.Fatal(err)
	}
	if seen != 14 {
		t.Fatalf("expected resumed traversal to visit 14 nodes, got %d", seen)
	}

	// resume from middle.
	resumer.RewindToOffset(17)
	seen = 0
	if err := p.WalkAdv(rootNode, s, count); err != nil {
		t.Fatal(err)
	}
	if seen != 13 {
		t.Fatalf("expected resumed traversal to visit 13 nodes, got %d", seen)
	}

	// resume from just before the middle.
	resumer.RewindToOffset(127)
	seen = 0
	if err := p.WalkAdv(rootNode, s, count); err != nil {
		t.Fatal(err)
	}
	// will not visit 'linkedString' or 'linkedMap' before linked list.
	if seen != 13 {
		t.Fatalf("expected resumed traversal to visit 13 nodes, got %d", seen)
	}

	// resume from middle.
	resumer.RewindToOffset(128)
	seen = 0
	if err := p.WalkAdv(rootNode, s, count); err != nil {
		t.Fatal(err)
	}
	// will not visit 'linkedString' or 'linkedMap' before linked list.
	if seen != 7 {
		t.Fatalf("expected resumed traversal to visit 7 nodes, got %d", seen)
	}
}
