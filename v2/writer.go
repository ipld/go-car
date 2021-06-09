package car

import (
	"bytes"
	"context"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	car_v1 "github.com/ipld/go-car"
	"github.com/willscott/carbs"
	"io"
)

type (
	// Writer writes CAR v2 into a give io.Writer.
	Writer struct {
		Walk         car_v1.WalkFunc
		IndexCodec   carbs.IndexCodec
		NodeGetter   format.NodeGetter
		IndexPadding uint64
		w            io.Writer
	}
)

// NewWriter instantiates a new CAR v2 writer.
// The writer instantiated uses `carbs.IndexSorted` as the index codec,
// and `car_v1.DefaultWalkFunc` as the default walk function.
func NewWriter(ng format.NodeGetter, w io.Writer) *Writer {
	return &Writer{
		Walk:       car_v1.DefaultWalkFunc,
		IndexCodec: carbs.IndexSorted,
		NodeGetter: ng,
		w:          w,
	}
}

// Write writes the given root CIDs according to CAR v2 specification, traversing the DAG using the
// Writer#Walk function.
func (w *Writer) Write(ctx context.Context, roots []cid.Cid) (err error) {
	err = w.writePrefix()
	if err != nil {
		return
	}

	buf, err := w.encodeCarV1(ctx, roots)
	if err != nil {
		return
	}

	carV1Len := buf.Len()
	err = w.writeHeader(carV1Len)
	if err != nil {
		return
	}

	carV1Bytes := buf.Bytes()
	_, err = w.w.Write(carV1Bytes)
	if err != nil {
		return
	}
	return w.writeIndex(carV1Bytes)
}

func (w *Writer) writeHeader(carV1Len int) error {
	header := NewHeader(uint64(carV1Len)).WithPadding(w.IndexPadding)
	return header.Marshal(w.w)
}

func (w *Writer) writePrefix() error {
	_, err := w.w.Write(PrefixBytes)
	return err
}

func (w *Writer) encodeCarV1(ctx context.Context, roots []cid.Cid) (buf *bytes.Buffer, err error) {
	buf = new(bytes.Buffer)
	err = car_v1.WriteCarWithWalker(ctx, w.NodeGetter, roots, buf, w.Walk)
	return
}

func (w *Writer) writeIndex(carV1 []byte) (err error) {
	reader := bytes.NewReader(carV1)
	index, err := carbs.GenerateIndex(reader, int64(len(carV1)), carbs.IndexSorted, true)
	if err != nil {
		return
	}
	return index.Marshal(w.w)
}
