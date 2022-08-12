package car

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-car/v2/internal/carv1"
	ioint "github.com/ipld/go-car/v2/internal/io"
	"github.com/ipld/go-car/v2/internal/loader"
	resumetraversal "github.com/ipld/go-car/v2/traversal"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

// ErrSizeMismatch is returned when a written traversal realizes the written header size does not
// match the actual number of car bytes written.
var ErrSizeMismatch = fmt.Errorf("car-error-sizemismatch")

// ErrOffsetImpossible is returned when specified paddings or offsets of either a wrapped carv1
// or index cannot be satisfied based on the data being written.
var ErrOffsetImpossible = fmt.Errorf("car-error-offsetimpossible")

// MaxTraversalLinks changes the allowed number of links a selector traversal
// can execute before failing.
//
// Note that setting this option may cause an error to be returned from selector
// execution when building a SelectiveCar.
func MaxTraversalLinks(MaxTraversalLinks uint64) Option {
	return func(sco *Options) {
		sco.MaxTraversalLinks = MaxTraversalLinks
	}
}

// WithDataPayloadSize sets the expected v1 size of the car being written if it is known in advance.
// This is required if NewCarV1StreamReader() is used and a Seek() operation needs seek back from
// SeekEnd (i.e. if we don't know where the end is, we can't figure out how far that is from the start).
// It can also be used to validate the expected size of a CAR's data payload if it's known in advance. In
// that case, a selective CAR creation operation will return an ErrSizeMismatch if the actual size doesn't
// match the expected set with this option.
func WithDataPayloadSize(size uint64) Option {
	return func(sco *Options) {
		sco.DataPayloadSize = size
	}
}

// NewSelectiveWriter walks through the proposed dag traversal to learn its total size in order to be able to
// stream out a car to a writer in the expected traversal order in one go.
func NewSelectiveWriter(ctx context.Context, ls *ipld.LinkSystem, root cid.Cid, selector ipld.Node, opts ...Option) (Writer, error) {
	conf := ApplyOptions(opts...)
	if conf.DataPayloadSize != 0 {
		return &traversalCar{
			size:     conf.DataPayloadSize,
			ctx:      ctx,
			root:     root,
			selector: selector,
			ls:       ls,
			opts:     ApplyOptions(opts...),
		}, nil
	}
	tc := traversalCar{
		//size:     headSize + cntr.Size(),
		ctx:      ctx,
		root:     root,
		selector: selector,
		ls:       ls,
		opts:     ApplyOptions(opts...),
	}
	if err := tc.setup(ctx, ls, ApplyOptions(opts...)); err != nil {
		return nil, err
	}

	c1h := carv1.CarHeader{Roots: []cid.Cid{root}, Version: 1}
	headSize, err := carv1.HeaderSize(&c1h)
	if err != nil {
		return nil, err
	}
	if err := tc.traverse(root, selector); err != nil {
		return nil, err
	}
	tc.size = headSize + tc.resumer.Position()
	return &tc, nil
}

// TraverseToFile writes a car file matching a given root and selector to the
// path at `destination` using one read of each block.
func TraverseToFile(ctx context.Context, ls *ipld.LinkSystem, root cid.Cid, selector ipld.Node, destination string, opts ...Option) error {
	conf := ApplyOptions(opts...)
	tc := traversalCar{
		size:     conf.DataPayloadSize,
		ctx:      ctx,
		root:     root,
		selector: selector,
		ls:       ls,
		opts:     conf,
	}

	fp, err := os.Create(destination)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = tc.WriteTo(fp)
	if err != nil {
		return err
	}

	// fix header size.
	if _, err = fp.Seek(0, 0); err != nil {
		return err
	}

	tc.size = uint64(tc.size)
	if _, err = tc.WriteV2Header(fp); err != nil {
		return err
	}

	return nil
}

// TraverseV1 walks through the proposed dag traversal and writes a carv1 to the provided io.Writer
func TraverseV1(ctx context.Context, ls *ipld.LinkSystem, root cid.Cid, selector ipld.Node, writer io.Writer, opts ...Option) (uint64, error) {
	opts = append(opts, WithoutIndex())
	conf := ApplyOptions(opts...)
	tc := traversalCar{
		size:     conf.DataPayloadSize,
		ctx:      ctx,
		root:     root,
		selector: selector,
		ls:       ls,
		opts:     conf,
	}

	len, _, err := tc.WriteV1(tc.ctx, conf.SkipOffset, writer)
	return len, err
}

// NewSelectiveV1Reader creates an io.ReadSeeker that can be used to stream a
// CARv1 given a LinkSystem, root CID and a selector. If Seek() is used, the
// output will only be given from that point in the resulting CAR. Where the
// size of the CAR is known ahead of time and provided via the
// WithDataPayloadSize option, seeking from the end of the CAR is permissible.
func NewSelectiveV1Reader(ctx context.Context, ls *ipld.LinkSystem, root cid.Cid, selector ipld.Node, opts ...Option) (io.ReadSeeker, error) {
	opts = append(opts, WithoutIndex())
	conf := ApplyOptions(opts...)
	tc := traversalCar{
		size:     conf.DataPayloadSize,
		ctx:      ctx,
		root:     root,
		selector: selector,
		ls:       ls,
		opts:     conf,
	}
	rwf := func(ctx context.Context, offset uint64, writer io.Writer) (uint64, error) {
		// it's only at this point we have the `offset` to start writing at since the user of the
		// ReadSeeker will (may) have called Seek() and we've worked out where in the CAR
		// that is as an offset. Now we can start writing the CARv1 data, which will be passed
		// on to the ReadSeeker.
		// Note that we're inside a goroutine here
		s, _, err := tc.WriteV1(ctx, offset, writer)
		return s, err
	}
	rw := ioint.NewSkipWriterReaderSeeker(ctx, conf.DataPayloadSize, rwf)
	return rw, nil
}

// Writer is an interface allowing writing a car prepared by PrepareTraversal
type Writer interface {
	io.WriterTo
}

var _ Writer = (*traversalCar)(nil)

type traversalCar struct {
	size     uint64
	ctx      context.Context
	root     cid.Cid
	selector ipld.Node
	ls       *ipld.LinkSystem
	opts     Options
	progress *traversal.Progress
	resumer  resumetraversal.TraverseResumer
}

func (tc *traversalCar) WriteTo(w io.Writer) (int64, error) {
	n, err := tc.WriteV2Header(w)
	if err != nil {
		return n, err
	}
	v1s, idx, err := tc.WriteV1(tc.ctx, 0, w)
	n += int64(v1s)

	if err != nil {
		return n, err
	}

	// index padding, then index
	if tc.opts.IndexCodec != index.CarIndexNone {
		if tc.opts.IndexPadding > 0 {
			buf := make([]byte, tc.opts.IndexPadding)
			pn, err := w.Write(buf)
			n += int64(pn)
			if err != nil {
				return n, err
			}
		}
		in, err := index.WriteTo(idx, w)
		n += int64(in)
		if err != nil {
			return n, err
		}
	}

	return n, err
}

func (tc *traversalCar) WriteV2Header(w io.Writer) (int64, error) {
	n, err := w.Write(Pragma)
	if err != nil {
		return int64(n), err
	}

	h := NewHeader(tc.size)
	if p := tc.opts.DataPadding; p > 0 {
		h = h.WithDataPadding(p)
	}
	if p := tc.opts.IndexPadding; p > 0 {
		h = h.WithIndexPadding(p)
	}
	if tc.opts.IndexCodec == index.CarIndexNone {
		h.IndexOffset = 0
	}
	hn, err := h.WriteTo(w)
	if err != nil {
		return int64(n) + hn, err
	}
	hn += int64(n)

	// We include the initial data padding after the carv2 header
	if h.DataOffset > uint64(hn) {
		// TODO: buffer writes if this needs to be big.
		buf := make([]byte, h.DataOffset-uint64(hn))
		n, err = w.Write(buf)
		hn += int64(n)
		if err != nil {
			return hn, err
		}
	} else if h.DataOffset < uint64(hn) {
		return hn, ErrOffsetImpossible
	}

	return hn, nil
}

// WriteV1 writes a v1 car to the writer, w, except for the first `skip` bytes.
// Returns bytes written, an index of what was written, or error if one occured.
func (tc *traversalCar) WriteV1(ctx context.Context, skip uint64, w io.Writer) (uint64, index.Index, error) {
	written := uint64(0)

	// write the v1 header
	c1h := carv1.CarHeader{Roots: []cid.Cid{tc.root}, Version: 1}
	v1Size, err := carv1.HeaderSize(&c1h)
	if err != nil {
		return 0, nil, err
	}
	if skip < v1Size {
		buf := bytes.NewBuffer(nil)
		if err := carv1.WriteHeader(&c1h, buf); err != nil {
			return 0, nil, err
		}
		if _, err := w.Write(buf.Bytes()[skip:]); err != nil {
			return 0, nil, err
		}
		written = v1Size - skip
		skip = 0
	} else {
		skip -= v1Size
	}

	// write the blocks
	wls, writer := loader.TeeingLinkSystem(*tc.ls, w, v1Size, skip, tc.opts.IndexCodec)
	if err = tc.setup(ctx, &wls, tc.opts); err != nil {
		return v1Size, nil, err
	}
	err = tc.traverse(tc.root, tc.selector)
	v1Size = writer.Size() - v1Size + written
	if err != nil {
		return v1Size, nil, err
	}
	if tc.size != 0 && tc.size != v1Size {
		return v1Size, nil, ErrSizeMismatch
	}
	tc.size = v1Size

	if tc.opts.IndexCodec == index.CarIndexNone {
		return v1Size, nil, nil
	}
	idx, err := writer.Index()
	return v1Size, idx, err
}

func (tc *traversalCar) setup(ctx context.Context, ls *ipld.LinkSystem, opts Options) error {
	chooser := func(_ ipld.Link, _ linking.LinkContext) (ipld.NodePrototype, error) {
		return basicnode.Prototype.Any, nil
	}
	if opts.TraversalPrototypeChooser != nil {
		chooser = opts.TraversalPrototypeChooser
	}

	progress := traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     *ls,
			LinkTargetNodePrototypeChooser: chooser,
			LinkVisitOnlyOnce:              !opts.BlockstoreAllowDuplicatePuts,
		},
	}
	if opts.MaxTraversalLinks < math.MaxInt64 {
		progress.Budget = &traversal.Budget{
			NodeBudget: math.MaxInt64,
			LinkBudget: int64(opts.MaxTraversalLinks),
		}
	}

	ls.TrustedStorage = true
	resumer, err := resumetraversal.WithTraversingLinksystem(&progress)
	if err != nil {
		return err
	}
	tc.progress = &progress
	tc.resumer = resumer
	return nil
}

func (tc *traversalCar) traverse(root cid.Cid, s ipld.Node) error {
	sel, err := selector.CompileSelector(s)
	if err != nil {
		return err
	}
	lnk := cidlink.Link{Cid: root}
	rp, err := tc.progress.Cfg.LinkTargetNodePrototypeChooser(lnk, ipld.LinkContext{})
	if err != nil {
		return err
	}
	rootNode, err := tc.progress.Cfg.LinkSystem.Load(ipld.LinkContext{}, lnk, rp)
	if err != nil {
		return fmt.Errorf("root blk load failed: %s", err)
	}
	err = tc.progress.WalkMatching(rootNode, sel, func(_ traversal.Progress, node ipld.Node) error {
		if lbn, ok := node.(datamodel.LargeBytesNode); ok {
			s, err := lbn.AsLargeBytes()
			if err != nil {
				return err
			}
			_, err = io.Copy(ioutil.Discard, s)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk failed: %s", err)
	}
	return nil
}
