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
	// Padding represents the number of padding bytes.
	Padding uint64
	// Writer writes CAR v2 into a give io.Writer.
	Writer struct {
		Walk         car_v1.WalkFunc
		IndexCodec   carbs.IndexCodec
		NodeGetter   format.NodeGetter
		CarV1Padding Padding
		IndexPadding Padding

		ctx   context.Context
		roots []cid.Cid
	}
)

// WriteTo writes this padding to the given writer as default value bytes.
func (p Padding) WriteTo(w io.Writer) (n int64, err error) {
	paddingBytes := make([]byte, p)
	written, err := w.Write(paddingBytes)
	n = int64(written)
	return
}

// NewWriter instantiates a new CAR v2 writer.
// The writer instantiated uses `carbs.IndexSorted` as the index codec,
// and `car_v1.DefaultWalkFunc` as the default walk function.
func NewWriter(ctx context.Context, ng format.NodeGetter, roots []cid.Cid) *Writer {
	return &Writer{
		Walk:       car_v1.DefaultWalkFunc,
		IndexCodec: carbs.IndexSorted,
		NodeGetter: ng,
		ctx:        ctx,
		roots:      roots,
	}
}

// WriteTo writes the given root CIDs according to CAR v2 specification, traversing the DAG using the
// Writer#Walk function.
func (w *Writer) WriteTo(writer io.Writer) (n int64, err error) {
	n, err = w.writePrefix(writer)
	if err != nil {
		return
	}
	// We read the entire car into memory because carbs#GenerateIndex takes a reader.
	// Future PRs will make this more efficient by exposing necessary interfaces in carbs so that
	// this can be done in an streaming manner.
	buf, err := w.encodeCarV1()
	if err != nil {
		return
	}
	carV1Len := buf.Len()

	wn, err := w.writeHeader(writer, carV1Len)
	if err != nil {
		return
	}
	n += wn

	wn, err = w.CarV1Padding.WriteTo(writer)
	if err != nil {
		return
	}
	n += wn

	carV1Bytes := buf.Bytes()
	wwn, err := writer.Write(carV1Bytes)
	if err != nil {
		return
	}
	n += int64(wwn)

	wn, err = w.IndexPadding.WriteTo(writer)
	if err != nil {
		return
	}
	n += wn

	wn, err = w.writeIndex(writer, carV1Bytes)
	if err == nil {
		n += wn
	}
	return
}

func (w *Writer) writeHeader(writer io.Writer, carV1Len int) (int64, error) {
	header := NewHeader(uint64(carV1Len)).
		WithCarV1Padding(w.CarV1Padding).
		WithIndexPadding(w.IndexPadding)
	return header.WriteTo(writer)
}

func (w *Writer) writePrefix(writer io.Writer) (int64, error) {
	n, err := writer.Write(PrefixBytes)
	return int64(n), err
}

func (w *Writer) encodeCarV1() (buf *bytes.Buffer, err error) {
	buf = new(bytes.Buffer)
	err = car_v1.WriteCarWithWalker(w.ctx, w.NodeGetter, w.roots, buf, w.Walk)
	return
}

func (w *Writer) writeIndex(writer io.Writer, carV1 []byte) (n int64, err error) {
	reader := bytes.NewReader(carV1)
	index, err := carbs.GenerateIndex(reader, int64(len(carV1)), carbs.IndexSorted, true)
	if err != nil {
		return
	}
	err = index.Marshal(writer)
	// FIXME refactor carbs to expose the number of bytes written.
	return
}
