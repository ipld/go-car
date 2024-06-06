package lib

import (
	"os"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
)

// CarRoot prints the root CID in a car
func CarRoot(file string) (roots []cid.Cid, err error) {
	inStream := os.Stdin
	if len(file) >= 1 {
		inStream, err = os.Open(file)
		if err != nil {
			return nil, err
		}
	}

	rd, err := carv2.NewBlockReader(inStream)
	if err != nil {
		return nil, err
	}
	return rd.Roots, nil
}
