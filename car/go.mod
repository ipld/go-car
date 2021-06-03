module github.com/ipld/go-car/car

go 1.15

require (
	github.com/ipld/go-car v0.3.0
	github.com/urfave/cli v1.22.5
)

// Only applies when built by checking out the source directory.
replace github.com/ipld/go-car => ./..
