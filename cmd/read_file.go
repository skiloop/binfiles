package main

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/alecthomas/kong"
	"os"
)

var cli struct {
	binFile string `arg:"" help:"source file"`
}

func main() {
	_ = kong.Parse(&cli)
	w, err := os.OpenFile(cli.binFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	var num int32
	num = 4
	err = binary.Write(w, binary.LittleEndian, num)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	_ = w.Close()

	r, err := os.OpenFile(cli.binFile, os.O_RDONLY, 0)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	var n int32
	err = binary.Read(r, binary.LittleEndian, &n)

	bw := bytes.Buffer{}
	wt := gzip.NewWriter(&bw)
	_, _ = wt.Write(make([]byte, 64))
	_ = wt.Flush()
	_ = wt.Close()
	fmt.Println(base64.StdEncoding.EncodeToString(bw.Bytes()))
}
