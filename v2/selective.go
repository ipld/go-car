package car

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/internal/carv1"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/multiformats/go-varint"
)

const ErrSizeMismatch = "car-error-sizemismatch"

// PrepareTraversal walks through the proposed dag traversal to learn it's total size in order to be able to
// stream out a car to a writer in the expected traversal order in one go.
func PrepareTraversal(ctx context.Context, ls *ipld.LinkSystem, root cid.Cid, selector ipld.Node, opts ...Option) (Traversal, error) {
	cls, cntr := countingLinkSystem(*ls)

	c1h := carv1.CarHeader{Roots: []cid.Cid{root}, Version: 1}
	headSize, err := carv1.HeaderSize(&c1h)
	if err != nil {
		return nil, err
	}
	if err := traverse(ctx, &cls, root, selector, opts...); err != nil {
		return nil, err
	}
	tc := traversalCar{
		size:     headSize + cntr.totalRead,
		ctx:      ctx,
		root:     root,
		selector: selector,
		ls:       ls,
		opts:     opts,
	}
	return &tc, nil
}

func FileTraversal(ctx context.Context, ls *ipld.LinkSystem, root cid.Cid, selector ipld.Node, destination string, opts ...Option) error {
	tc := traversalCar{
		size:     0,
		ctx:      ctx,
		root:     root,
		selector: selector,
		ls:       ls,
		opts:     opts,
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
	if _, err = tc.WriteHeader(fp); err != nil {
		return err
	}

	return nil
}

// Traversal is a allows writing a car with the data specified by a selector.
type Traversal interface {
	io.WriterTo
}

type traversalCar struct {
	size     uint64
	ctx      context.Context
	root     cid.Cid
	selector ipld.Node
	ls       *ipld.LinkSystem
	opts     []Option
}

func (tc *traversalCar) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(Pragma)
	if err != nil {
		return int64(n), err
	}
	h, err := tc.WriteHeader(w)
	if err != nil {
		return int64(n) + h, err
	}
	h += int64(n)

	// write the v1 header
	c1h := carv1.CarHeader{Roots: []cid.Cid{tc.root}, Version: 1}
	if err := carv1.WriteHeader(&c1h, w); err != nil {
		return h, err
	}
	hn, err := carv1.HeaderSize(&c1h)
	h += int64(hn)
	if err != nil {
		return h, err
	}
	v1Size := hn

	// write the block.
	wls, writer := teeingLinkSystem(*tc.ls, w)
	err = traverse(tc.ctx, &wls, tc.root, tc.selector, tc.opts...)
	h += int64(writer.size)
	v1Size += writer.size
	if err != nil {
		return h, err
	}
	if tc.size != 0 && tc.size != v1Size {
		return h, fmt.Errorf(ErrSizeMismatch)
	}
	tc.size = v1Size

	return h, nil
}

func (tc *traversalCar) WriteHeader(w io.Writer) (int64, error) {
	h := NewHeader(tc.size)
	// TODO: support calculation / inclusion of the index.
	h.IndexOffset = 0
	return h.WriteTo(w)
}

type counter struct {
	totalRead uint64
}

type countingReader struct {
	r    io.Reader
	c    *counter
	read uint64
	cid  string
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if err == io.EOF {
		// add in the overall length of the block.
		n += len(c.cid)
		uv := varint.ToUvarint(uint64(n))
		n += len(uv)
	}
	c.c.totalRead += uint64(n)
	c.read += uint64(n)
	return n, err
}

func countingLinkSystem(ls ipld.LinkSystem) (ipld.LinkSystem, *counter) {
	c := counter{}
	return linking.LinkSystem{
		EncoderChooser:     ls.EncoderChooser,
		DecoderChooser:     ls.DecoderChooser,
		HasherChooser:      ls.HasherChooser,
		StorageWriteOpener: ls.StorageWriteOpener,
		StorageReadOpener: func(lc linking.LinkContext, l ipld.Link) (io.Reader, error) {
			r, err := ls.StorageReadOpener(lc, l)
			if err != nil {
				return nil, err
			}
			return &countingReader{r, &c, 0, l.Binary()}, nil
		},
		TrustedStorage: ls.TrustedStorage,
		NodeReifier:    ls.NodeReifier,
	}, &c
}

func traverse(ctx context.Context, ls *ipld.LinkSystem, root cid.Cid, s ipld.Node, opts ...Option) error {
	sel, err := selector.CompileSelector(s)
	if err != nil {
		return err
	}

	progress := traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:        ctx,
			LinkSystem: *ls,
			LinkTargetNodePrototypeChooser: func(_ ipld.Link, _ linking.LinkContext) (ipld.NodePrototype, error) {
				return basicnode.Prototype.Any, nil
			},
			LinkVisitOnlyOnce: true, // TODO: from opts,
		},
	}

	lnk := cidlink.Link{Cid: root}
	rootNode, err := ls.Load(ipld.LinkContext{}, lnk, basicnode.Prototype.Any)
	if err != nil {
		return err
	}
	return progress.WalkMatching(rootNode, sel, func(_ traversal.Progress, _ ipld.Node) error {
		return nil
	})
}

type writerOutput struct {
	w    io.Writer
	size uint64
}

type writingReader struct {
	r   io.Reader
	len int64
	cid string
	wo  *writerOutput
}

func (w *writingReader) Read(p []byte) (int, error) {
	if w.wo != nil {
		// write the cid
		size := varint.ToUvarint(uint64(w.len))
		if _, err := w.wo.w.Write(size); err != nil {
			return 0, err
		}
		if _, err := w.wo.w.Write([]byte(w.cid)); err != nil {
			return 0, err
		}
		cpy := bytes.NewBuffer(w.r.(*bytes.Buffer).Bytes())
		if _, err := cpy.WriteTo(w.wo.w); err != nil {
			return 0, err
		}
		w.wo = nil
	}

	return w.r.Read(p)
}

func teeingLinkSystem(ls ipld.LinkSystem, w io.Writer) (ipld.LinkSystem, *writerOutput) {
	wo := writerOutput{w, 0}
	return linking.LinkSystem{
		EncoderChooser:     ls.EncoderChooser,
		DecoderChooser:     ls.DecoderChooser,
		HasherChooser:      ls.HasherChooser,
		StorageWriteOpener: ls.StorageWriteOpener,
		StorageReadOpener: func(lc linking.LinkContext, l ipld.Link) (io.Reader, error) {
			r, err := ls.StorageReadOpener(lc, l)
			if err != nil {
				return nil, err
			}
			buf := bytes.NewBuffer(nil)
			n, err := buf.ReadFrom(r)
			if err != nil {
				return nil, err
			}
			return &writingReader{buf, n, l.Binary(), &wo}, nil
		},
		TrustedStorage: ls.TrustedStorage,
		NodeReifier:    ls.NodeReifier,
	}, &wo
}
