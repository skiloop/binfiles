package binfile

import (
	"fmt"
	"os"
)

func open2read(filename string) (*os.File, error) {
	fn, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to open %s: %v\n", filename, err)
		return nil, err
	}
	return fn, nil
}
