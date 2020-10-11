package main

import (
	"fmt"
	"os"

	"github.com/willscott/carbs"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: hydrate <file.car> [codec]\n")
		return
	}
	db := os.Args[1]
	codec := carbs.IndexSorted
	if len(os.Args) == 3 {
		if os.Args[2] == "Hash" {
			codec = carbs.IndexHashed
		}
	}

	if err := carbs.Generate(db, codec); err != nil {
		fmt.Printf("Error Hydrating: %v\n", err)
	}
	return
}
