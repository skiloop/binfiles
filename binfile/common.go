package binfile

import (
	"io"
	"os"
)

var Verbose = false

const (
	GZIP = iota
	NONE // do not compress
	ZIP
	BZIP2
	BROTLI
	LZ4
	XZ
)

var KeySizeLimit int32 = 1000
var EmptyDocKey = "empty-doc."

const MaxDocSize = 0x40000000 // 1GB

var CompressTypes = map[string]int{
	"gzip":   GZIP,
	"none":   NONE,
	"zip":    ZIP,
	"bzip2":  BZIP2,
	"bz2":    BZIP2,
	"br":     BROTLI,
	"xz":     XZ,
	"brotli": BROTLI,
	"lz4":    LZ4,
}

type outWriter struct {
	file       *os.File
	compressor Compressor
}

func (o *outWriter) Write(p []byte) (n int, err error) {
	return o.compressor.Write(p)
}

func (o *outWriter) Close() error {
	if nil == o.file {
		return nil
	}
	_ = o.compressor.Flush()
	_ = o.compressor.Close()
	_ = o.file.Close()
	o.compressor = nil
	o.file = nil
	return nil
}

func newOutWriter(filename string, compressType int) (io.Writer, error) {
	if filename == "" {
		return os.Stdout, nil
	}
	file, err := os.OpenFile(filename, writerFileFlag, 0644)
	if err != nil {
		return nil, err
	}
	compressor, err := getCompressor(compressType, file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &outWriter{file: file, compressor: compressor}, nil
}
