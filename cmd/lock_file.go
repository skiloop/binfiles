package main

import (
	"fmt"
	"github.com/alecthomas/kong"
	"github.com/skiloop/binfiles/binfile"
	"github.com/skiloop/binfiles/binfile/filelock"
	"os"
)

var cc struct {
	Verbose bool   `short:"v" help:"verbose" default:"false"`
	Src     string `short:"s" help:"source file" default:""`
	Cmd     string `cmd:"" aliases:"c" help:"command to run"`
}

func main() {
	_ = kong.Parse(&cc)
	binfile.Verbose = cc.Verbose
	if cc.Src == "" {
		fmt.Printf("bin file is required")
		return
	}
	w, err := os.OpenFile(cc.Src, os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("open failed: %v\n", err)
		return
	}

	err = filelock.Lock(*w)
	if err != nil {
		fmt.Printf("lock failed: %v\n", err)
		return
	}
	defer func(f os.File) {
		err := filelock.UnLock(f)
		if err != nil {
			fmt.Printf("unlock failed: %v\n", err)
		}
	}(*w)

}
