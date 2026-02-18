package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/cmd/car/lib"
	"github.com/ipld/go-car/v2"
	carstorage "github.com/ipld/go-car/v2/storage"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage"
	"github.com/urfave/cli/v2"
)

var ErrNotDir = fmt.Errorf("not a directory")

// ExtractCar pulls files and directories out of a car
func ExtractCar(c *cli.Context) error {
	if !c.IsSet("file") {
		return fmt.Errorf("a file source must be specified")
	}

	outputDir, err := os.Getwd()
	if err != nil {
		return err
	}
	if c.IsSet("output") {
		outputDir = c.String("output")
	}

	var store storage.ReadableStorage
	var roots []cid.Cid

	carFile, err := os.Open(c.String("file"))
	if err != nil {
		return err
	}
	store, err = carstorage.OpenReadable(carFile)
	if err != nil {
		return err
	}
	roots = store.(carstorage.ReadableCar).Roots()

	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true
	ls.SetReadStorage(store)

	paths := c.Args().Slice()
	if len(paths) == 0 {
		paths = append(paths, "")
	}

	var extractedFiles int

	for _, p := range paths {
		path, err := pathSegments(p)
		if err != nil {
			return err
		}

		for _, root := range roots {
			count, err := lib.ExtractToDir(c.Context, &ls, root, outputDir, path, c.IsSet("verbose"), c.App.ErrWriter)
			if err != nil {
				return err
			}
			extractedFiles += count
		}
	}

	if extractedFiles == 0 {
		return cli.Exit("no files extracted", 1)
	} else {
		fmt.Fprintf(c.App.ErrWriter, "extracted %d file(s)\n", extractedFiles)
	}

	return nil
}

// TODO: dedupe this with lassie, probably into go-unixfsnode
func pathSegments(path string) ([]string, error) {
	segments := strings.Split(path, "/")
	filtered := make([]string, 0, len(segments))
	for i := 0; i < len(segments); i++ {
		if segments[i] == "" {
			// Allow one leading and one trailing '/' at most
			if i == 0 || i == len(segments)-1 {
				continue
			}
			return nil, fmt.Errorf("invalid empty path segment at position %d", i)
		}
		if segments[i] == "." || segments[i] == ".." {
			return nil, fmt.Errorf("'%s' is unsupported in paths", segments[i])
		}
		filtered = append(filtered, segments[i])
	}
	return filtered, nil
}

var _ storage.ReadableStorage = (*stdinReadStorage)(nil)

type stdinReadStorage struct {
	blocks map[string][]byte
	done   bool
	lk     *sync.RWMutex
	cond   *sync.Cond
}

func NewStdinReadStorage(reader io.Reader) (*stdinReadStorage, []cid.Cid, error) {
	var lk sync.RWMutex
	srs := &stdinReadStorage{
		blocks: make(map[string][]byte),
		lk:     &lk,
		cond:   sync.NewCond(&lk),
	}
	rdr, err := car.NewBlockReader(reader)
	if err != nil {
		return nil, nil, err
	}
	go func() {
		for {
			blk, err := rdr.Next()
			if err == io.EOF {
				srs.lk.Lock()
				srs.done = true
				srs.cond.Broadcast()
				srs.lk.Unlock()
				return
			}
			if err != nil {
				panic(err)
			}
			srs.lk.Lock()
			srs.blocks[string(blk.Cid().Hash())] = blk.RawData()
			srs.cond.Broadcast()
			srs.lk.Unlock()
		}
	}()
	return srs, rdr.Roots, nil
}

func (srs *stdinReadStorage) Has(ctx context.Context, key string) (bool, error) {
	_, err := srs.Get(ctx, key)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (srs *stdinReadStorage) Get(ctx context.Context, key string) ([]byte, error) {
	c, err := cid.Cast([]byte(key))
	if err != nil {
		return nil, err
	}
	srs.lk.Lock()
	defer srs.lk.Unlock()
	for {
		if data, ok := srs.blocks[string(c.Hash())]; ok {
			return data, nil
		}
		if srs.done {
			return nil, carstorage.ErrNotFound{Cid: c}
		}
		srs.cond.Wait()
	}
}
