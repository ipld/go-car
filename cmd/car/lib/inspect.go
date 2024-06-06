package lib

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/multiformats/go-multicodec"
)

type Stat struct {
	Min, Mean, Max uint64
}

func (s Stat) String() string {
	return fmt.Sprintf("%d / %d / %d", s.Min, s.Mean, s.Max)
}

type Roots []string

func (r Roots) String() string {
	var roots strings.Builder
	switch len(r) {
	case 0:
		roots.WriteString(" (none)")
	case 1:
		roots.WriteString(" ")
		roots.WriteString(r[0])
	default:
		for _, root := range r {
			roots.WriteString("\n\t")
			roots.WriteString(root)
		}
	}
	return roots.String()
}

type Counts map[multicodec.Code]uint64

func (cs Counts) String() string {
	var codecs strings.Builder
	{
		keys := make([]int, len(cs))
		i := 0
		for codec := range cs {
			keys[i] = int(codec)
			i++
		}
		sort.Ints(keys)
		for _, code := range keys {
			codec := multicodec.Code(code)
			codecs.WriteString(fmt.Sprintf("\n\t%s: %d", codec, cs[codec]))
		}
	}
	return codecs.String()
}

type Report struct {
	Characteristics []byte
	DataOffset      uint64
	DataLength      uint64
	IndexOffset     uint64
	IndexType       string
	Version         int
	Roots           Roots
	RootsPresent    bool
	BlockCount      uint64
	BlkLength       Stat
	CidLength       Stat
	Codecs          Counts
	Hashes          Counts
}

func (r *Report) String() string {
	var v2s string
	if r.Version == 2 {
		v2s = fmt.Sprintf(`Characteristics: %x
Data offset: %d
Data (payload) length: %d
Index offset: %d
Index type: %s
`, r.Characteristics, r.DataOffset, r.DataLength, r.IndexOffset, r.IndexType)
	}

	rp := "No"
	if r.RootsPresent {
		rp = "Yes"
	}

	pfmt := `Version: %d
%sRoots:%s
Root blocks present in data: %s
Block count: %d
Min / average / max block length (bytes): %s
Min / average / max CID length (bytes): %s
Block count per codec:%s
CID count per multihash:%s
`

	return fmt.Sprintf(
		pfmt,
		r.Version,
		v2s,
		r.Roots.String(),
		rp,
		r.BlockCount,
		r.BlkLength.String(),
		r.CidLength.String(),
		r.Codecs.String(),
		r.Hashes.String(),
	)
}

func InspectCar(inStream *os.File, verifyHashes bool) (*Report, error) {
	rd, err := carv2.NewReader(inStream, carv2.ZeroLengthSectionAsEOF(true))
	if err != nil {
		return nil, err
	}
	stats, err := rd.Inspect(verifyHashes)
	if err != nil {
		return nil, err
	}

	if stats.Version == 1 && verifyHashes { // check that we've read all the data
		got, err := inStream.Read(make([]byte, 1)) // force EOF
		if err != nil && err != io.EOF {
			return nil, err
		} else if got > 0 {
			return nil, fmt.Errorf("unexpected data after EOF: %d", got)
		}
	}

	rep := Report{
		Version:      int(stats.Version),
		Roots:        []string{},
		RootsPresent: stats.RootsPresent,
		BlockCount:   stats.BlockCount,
		BlkLength:    Stat{Min: stats.MinBlockLength, Mean: stats.AvgBlockLength, Max: stats.MaxBlockLength},
		CidLength:    Stat{Min: stats.MinCidLength, Mean: stats.AvgCidLength, Max: stats.MaxCidLength},
		Codecs:       stats.CodecCounts,
		Hashes:       stats.MhTypeCounts,
	}

	for _, c := range stats.Roots {
		rep.Roots = append(rep.Roots, c.String())
	}

	if stats.Version == 2 {
		idx := "(none)"
		if stats.IndexCodec != 0 {
			idx = stats.IndexCodec.String()
		}
		var buf bytes.Buffer
		stats.Header.Characteristics.WriteTo(&buf)
		rep.Characteristics = buf.Bytes()
		rep.DataOffset = stats.Header.DataOffset
		rep.DataLength = stats.Header.DataSize
		rep.IndexOffset = stats.Header.IndexOffset
		rep.IndexType = idx
	}

	return &rep, nil
}
