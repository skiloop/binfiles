package binfile

import (
	"compress/gzip"
	"fmt"
	"github.com/andybalholm/brotli"
	"github.com/dsnet/compress/bzip2"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
	"io"
	"os"
)

var Verbose = false
var Debug = false

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

func getDecompressReader(ct int, src io.Reader) (reader io.Reader, err error) {
	switch ct {
	case NONE:
		return NewNoneCompressReader(src), nil
	case LZ4:
		return lz4.NewReader(src), nil
	case BROTLI:
		return brotli.NewReader(src), nil
	case XZ:
		return xz.NewReader(src)
	case BZIP2:
		return bzip2.NewReader(src, nil)
	case GZIP:
		fallthrough
	default:
		return gzip.NewReader(src)
	}
}

func debug(format string, a ...any) {
	if Debug {
		fmt.Printf(format, a...)
	}
}

func errorf(format string, a ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format, a...)
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
