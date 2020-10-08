package main

import (
	"fmt"
	"os"

	"github.com/willscott/carbs"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: hydrate <file.car>\n")
		return
	}
	db := os.Args[1]

	if err := carbs.Generate(db); err != nil {
		fmt.Printf("Error Hydrating: %v\n", err)
	}
	return
}
