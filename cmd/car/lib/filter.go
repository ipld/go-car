package lib

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
)

func FilterCar(ctx context.Context, infile, outfile string, cidMap map[cid.Cid]struct{}, invert bool, outVersion int, appendOutFile bool) error {
	fd, err := os.Open(infile)
	if err != nil {
		return err
	}
	defer fd.Close()
	rd, err := carv2.NewBlockReader(fd)
	if err != nil {
		return err
	}

	outRoots := make([]cid.Cid, 0)
	for _, r := range rd.Roots {
		if matchFilter(r, cidMap, invert) {
			outRoots = append(outRoots, r)
		}
	}

	options := []carv2.Option{}
	switch outVersion {
	case 1:
		options = []carv2.Option{blockstore.WriteAsCarV1(true)}
	case 2:
		// already the default
	default:
		return fmt.Errorf("invalid CAR version %d", outVersion)
	}

	if !appendOutFile {
		if _, err := os.Stat(outfile); err == nil || !os.IsNotExist(err) {
			// output to an existing file.
			if err := os.Truncate(outfile, 0); err != nil {
				return err
			}
		}
	} else {
		if outVersion != 2 {
			return fmt.Errorf("can only append to version 2 car files")
		}

		// roots will need to be whatever is in the output already.
		cv2r, err := carv2.OpenReader(outfile)
		if err != nil {
			return err
		}
		if cv2r.Version != 2 {
			return fmt.Errorf("can only append to version 2 car files")
		}
		outRoots, err = cv2r.Roots()
		if err != nil {
			return err
		}
		_ = cv2r.Close()
	}

	if len(outRoots) == 0 {
		fmt.Fprintf(os.Stderr, "warning: no roots defined after filtering\n")
	}

	bs, err := blockstore.OpenReadWrite(outfile, outRoots, options...)
	if err != nil {
		return err
	}

	for {
		blk, err := rd.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if matchFilter(blk.Cid(), cidMap, invert) {
			if err := bs.Put(ctx, blk); err != nil {
				return err
			}
		}
	}
	return bs.Finalize()
}

func matchFilter(c cid.Cid, cidMap map[cid.Cid]struct{}, invert bool) bool {
	if _, ok := cidMap[c]; ok {
		return !invert
	}
	return invert
}
