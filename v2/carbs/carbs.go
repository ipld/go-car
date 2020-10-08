package carbs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	bs "github.com/ipfs/go-ipfs-blockstore"

	car "github.com/ipld/go-car"
	cbor "github.com/whyrusleeping/cbor/go"
	"golang.org/x/exp/mmap"
)

// Carbs provides a read-only Car Block Store.
type Carbs struct {
	backing io.ReaderAt
	idx     *carbsIndex
}

var _ bs.Blockstore = (*Carbs)(nil)

func (c *Carbs) DeleteBlock(_ cid.Cid) error {
	return fmt.Errorf("read only")
}

func (c *Carbs) Has(key cid.Cid) (bool, error) {
	_, ok := (*(c.idx))[key]
	return ok, nil
}
func (c *Carbs) Get(cid.Cid) (blocks.Block, error) {

}

func (c *Carbs) GetSize(cid.Cid) (int, error) {

}

func (c *Carbs) Put(blocks.Block) error {
	return fmt.Errorf("read only")
}

func (c *Carbs) PutMany([]blocks.Block) error {
	return fmt.Errorf("read only")
}

func (c *Carbs) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {

}

func (c *Carbs) HashOnRead(enabled bool) {
	return
}

// Load opens a carbs data store, generating an index if it does not exist
func Load(path string, noPersist bool) (*Carbs, error) {
	reader, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}
	idx := make(carbsIndex)
	idxRef := &idx
	if err := idx.Unmarshal(path); err != nil {
		idxRef, err = generateIndex(reader)
		if err != nil {
			return nil, err
		}
		if !noPersist {
			if err = idxRef.Marshal(path); err != nil {
				return nil, err
			}
		}
	}
	obj := Carbs{
		backing: reader,
		idx:     idxRef,
	}
	return &obj, nil
}

type carbsIndex map[cid.Cid]uint

func (c carbsIndex) Marshal(path string) error {
	stream, err := os.OpenFile(path+".idx", os.O_WRONLY|os.O_APPEND, 0640)
	if err != nil {
		return err
	}
	defer stream.Close()
	return cbor.Encode(stream, &c)
}

func (c carbsIndex) Unmarshal(path string) error {
	stream, err := os.Open(path + ".idx")
	if err != nil {
		return err
	}
	defer stream.Close()
	decoder := cbor.NewDecoder(stream)
	return decoder.Decode(&c)
}

func generateIndex(store io.ReaderAt) (*carbsIndex, error) {
	header, err := car.ReadHeader(bufio.NewReader(unatreader{store, 0}))
	if err != nil {
		return nil, err
	}
	offset, err := car.HeaderSize(header)
	if err != nil {
		return nil, err
	}

	index := make(carbsIndex)

	return &index, nil
}

// Generate walks a car file and generates an index of cid->byte offset in it.
func Generate(path string) error {
	idx, err := generateIndex(store)
	if err != nil {
		return err
	}

	return nil
}
