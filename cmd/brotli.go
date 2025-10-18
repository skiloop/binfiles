package main

import (
	"io"
	"os"

	"github.com/alecthomas/kong"
	"github.com/andybalholm/brotli"
	"github.com/skiloop/binfiles/binfile"
)

var decompress struct {
	Input  string `arg:"" help:"source file"`
	Output string `arg:"" help:"source file"`
}

func main() {
	_ = kong.Parse(&decompress)
	var err error
	var r io.ReadCloser
	r, err = os.OpenFile(decompress.Input, os.O_RDONLY, 0644)
	if err != nil {
		binfile.LogInfo(err.Error())
		return
	}
	defer r.Close()
	var w io.WriteCloser
	w, err = os.OpenFile(decompress.Output, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		binfile.LogInfo(err.Error())
		return
	}
	defer w.Close()
	br := brotli.NewReader(r)

	var n int64
	n, err = io.Copy(w, br)
	if err != nil {
		binfile.LogError("decompress error: %d, %v\n", n, err)
	} else {
		binfile.LogInfo("decompression done with %d written\n", n)
	}
}
