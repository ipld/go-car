module github.com/ipld/go-car/cmd

go 1.16

require (
	github.com/dustin/go-humanize v1.0.0
	github.com/ipfs/go-block-format v0.0.3
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-ipfs-blockstore v1.0.3
	github.com/ipfs/go-unixfsnode v1.1.3
	github.com/ipld/go-car v0.3.2-0.20211001222544-c93f5367a75c
	github.com/ipld/go-car/v2 v2.0.3-0.20211001222544-c93f5367a75c
	github.com/ipld/go-codec-dagpb v1.3.0
	github.com/ipld/go-ipld-prime v0.12.3-0.20210930132912-0b3aef3ca569
	github.com/multiformats/go-multicodec v0.3.1-0.20210902112759-1539a079fd61
	github.com/multiformats/go-multihash v0.0.15
	github.com/multiformats/go-varint v0.0.6
	github.com/rogpeppe/go-internal v1.8.1-0.20210923151022-86f73c517451
	github.com/urfave/cli/v2 v2.3.0
)

replace github.com/ipfs/go-unixfsnode => ../../go-unixfsnode
