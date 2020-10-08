package carbs

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	bs "github.com/ipfs/go-ipfs-blockstore"

	car "github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	cbor "github.com/whyrusleeping/cbor/go"
	"golang.org/x/exp/mmap"
)

// Carbs provides a read-only Car Block Store.
type Carbs struct {
	backing io.ReaderAt
	idx     *carbsIndex
}

var _ bs.Blockstore = (*Carbs)(nil)

func (c *Carbs) Read(idx int64) ([]byte, error) {
	_, bytes, err := util.ReadNode(bufio.NewReader(unatreader{c.backing, idx}))
	return bytes, err
}

// DeleteBlock doesn't delete a block on RO blockstore
func (c *Carbs) DeleteBlock(_ cid.Cid) error {
	return fmt.Errorf("read only")
}

// Has indicates if the store has a cid
func (c *Carbs) Has(key cid.Cid) (bool, error) {
	_, ok := (*(c.idx))[key]
	return ok, nil
}

// Get gets a block from the store
func (c *Carbs) Get(key cid.Cid) (blocks.Block, error) {
	idx, ok := (*(c.idx))[key]
	if !ok {
		return nil, fmt.Errorf("no found")
	}
	bytes, err := c.Read(idx)
	if err != nil {
		return nil, err
	}
	return blocks.NewBlockWithCid(bytes, key)
}

// GetSize gets how big a item is
func (c *Carbs) GetSize(key cid.Cid) (int, error) {
	idx, ok := (*(c.idx))[key]
	if !ok {
		return 0, fmt.Errorf("not found")
	}
	len, err := binary.ReadUvarint(unatreader{c.backing, int64(idx)})
	return int(len), err
}

// Put does nothing on a ro store
func (c *Carbs) Put(blocks.Block) error {
	return fmt.Errorf("read only")
}

// PutMany does nothing on a ro store
func (c *Carbs) PutMany([]blocks.Block) error {
	return fmt.Errorf("read only")
}

// AllKeysChan returns the list of keys in the store
func (c *Carbs) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid, 5)
	go func() {
		done := ctx.Done()
		for key := range *(c.idx) {
			select {
			case ch <- key:
				continue
			case <-done:
				return
			}
		}
	}()
	return ch, nil
}

// HashOnRead does nothing
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

type carbsIndex map[cid.Cid]int64

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
	cidBuf := make([]byte, 48)

	rdr := unatreader{store, int64(offset)}
	for true {
		thisItemIdx := rdr.at
		l, err := binary.ReadUvarint(rdr)
		thisItemForNxt := rdr.at
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		m := l
		if 48 < l {
			m = 48
		}
		if _, err = rdr.Read(cidBuf[:m]); err != nil {
			return nil, err
		}
		c, _, err := util.ReadCid(cidBuf[:m])
		if err != nil {
			return nil, err
		}
		index[c] = thisItemIdx
		rdr.at = thisItemForNxt + int64(l)
	}

	return &index, nil
}

// Generate walks a car file and generates an index of cid->byte offset in it.
func Generate(path string) error {
	store, err := mmap.Open(path)
	if err != nil {
		return err
	}
	idx, err := generateIndex(store)
	if err != nil {
		return err
	}

	return idx.Marshal(path)
}
