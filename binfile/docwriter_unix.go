//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd

package binfile

import (
	"fmt"
	"os"
)

func (dw *docWriter) open() (err error) {
	if Debug {
		fmt.Printf("opening file %s for writing\n", dw.filename)
	}
	dw.file, err = os.OpenFile(dw.filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	return err
}
