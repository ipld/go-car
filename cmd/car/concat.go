package main

import (
	"fmt"
	"io"
	"os"

	carv1 "github.com/ipld/go-car"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/urfave/cli/v2"
)

// ConcatCar concatinates multiple car files into a single merged file.
func ConcatCar(c *cli.Context) (err error) {
	if c.Args().Len() < 1 {
		return fmt.Errorf("at least one car must be specified")
	}

	var outStream io.Writer
	if c.String("output") == "" {
		outStream = c.App.Writer
	} else {
		outStream, err = os.Create(c.String("output"))
		if err != nil {
			return err
		}
		defer outStream.(*os.File).Close()
	}

	first := true
	for _, arg := range c.Args().Slice() {
		inF, err := os.Open(arg)
		if err != nil {
			return fmt.Errorf("failed to open %s: %w", arg, err)
		}
		{
			defer inF.Close()

			cf, err := carv2.NewReader(inF)
			if err != nil {
				return fmt.Errorf("failed to open %s: %w", arg, err)
			}
			cv1, err := cf.DataReader()
			if err != nil {
				return fmt.Errorf("failed to open %s: %w", arg, err)
			}

			carReader, err := carv1.NewCarReader(cv1)
			if err != nil {
				return fmt.Errorf("failed to open %s: %w", arg, err)
			}
			offset, err := carv1.HeaderSize(carReader.Header)
			if err != nil {
				return fmt.Errorf("failed to open %s: %w", arg, err)
			}

			if first {
				if c.Int("version") == 2 {
					cf.Header.IndexOffset = 0
					if _, err := cf.Header.WriteTo(outStream); err != nil {
						return fmt.Errorf("failed to write header: %w", err)
					}
				}
				if err := carv1.WriteHeader(carReader.Header, outStream); err != nil {
					return fmt.Errorf("failed to write header: %w", err)
				}
				first = false
			}
			if _, err := cv1.Seek(int64(offset), io.SeekStart); err != nil {
				return fmt.Errorf("failed to seek %s: %w", arg, err)
			}
			if _, err := io.Copy(outStream, cv1); err != nil {
				return fmt.Errorf("failed to copy %s: %w", arg, err)
			}
		}
	}

	return nil
}
