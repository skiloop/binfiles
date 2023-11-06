//go:build windows

package binfile

import (
	"fmt"
	"os"
)

func (dw *docWriter) open() (err error) {
	if Debug {
		fmt.Printf("opening file %s for writing\n", dw.filename)
	}
	dw.file, err = os.OpenFile(dw.filename, os.O_APPEND|os.O_CREATE, 0o644)
	return err
}
