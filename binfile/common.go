package binfile

import (
	"compress/gzip"
	"github.com/andybalholm/brotli"
	"github.com/dsnet/compress/bzip2"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
	"io"
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

type brotliWriter struct {
	dst *brotli.Writer
}

func (bw *brotliWriter) Write(p []byte) (int, error) {
	defer func(dst *brotli.Writer) {
		_ = dst.Flush()
	}(bw.dst)
	return bw.dst.Write(p)
}

func (bw *brotliWriter) Close() error {
	return bw.dst.Close()
}

func newBrotliWriter(w io.Writer) *brotliWriter {
	return &brotliWriter{dst: brotli.NewWriter(w)}
}

type lz4Writer struct {
	dst *lz4.Writer
}

func (bw *lz4Writer) Write(p []byte) (int, error) {
	defer func(dst *lz4.Writer) {
		_ = dst.Flush()
	}(bw.dst)
	return bw.dst.Write(p)
}

func (bw *lz4Writer) Close() error {
	return bw.dst.Close()
}

func newLz4Writer(w io.Writer) *lz4Writer {
	return &lz4Writer{dst: lz4.NewWriter(w)}
}

func getCompressCloser(compressType int, w io.Writer) (io.WriteCloser, error) {
	switch compressType {
	case NONE:
		return NewNoneCompressWriter(w), nil
	case BZIP2:
		return bzip2.NewWriter(w, nil)
	case LZ4:
		return newLz4Writer(w), nil
	case BROTLI:
		return newBrotliWriter(w), nil
	case XZ:
		return xz.NewWriter(w)
	case GZIP:
		fallthrough
	default:
		return gzip.NewWriter(w), nil
	}
	//return nil, errors.New(fmt.Sprintf("unknown package compression type %d", compressType))
}
