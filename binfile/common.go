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

const workerEndFlag = "" // task worker end flag

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

func getCompressCloser(compressType int, w io.Writer) (io.WriteCloser, error) {
	switch compressType {
	case NONE:
		return NewNoneCompressWriter(w), nil
	case BZIP2:
		return bzip2.NewWriter(w, nil)
	case LZ4:
		return lz4.NewWriter(w), nil
	case BROTLI:
		return brotli.NewWriter(w), nil
	case XZ:
		return xz.NewWriter(w)
	case GZIP:
		fallthrough
	default:
		//return newFlushWriter(gzip.NewWriter(w)), nil
		return gzip.NewWriter(w), nil
	}
	//return nil, errors.New(fmt.Sprintf("unknown package compression type %d", compressType))
}
