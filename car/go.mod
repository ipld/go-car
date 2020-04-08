module github.com/ipld/go-car/car

go 1.13

require (
	github.com/ipld/go-car v0.1.0
	github.com/urfave/cli v1.22.4
)

// Only applies when built by checking out the source directory.
replace github.com/ipld/go-car => ./..
