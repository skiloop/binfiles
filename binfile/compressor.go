package binfile

import (
	"bytes"
	"compress/gzip"
	"github.com/andybalholm/brotli"
	"github.com/dsnet/compress/bzip2"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
	"io"
)

type flusher interface {
	Flush() error
}

type Compressor interface {
	flusher
	io.Writer
	io.Closer
}

type _compressor struct {
	w io.Writer
}

func (c _compressor) Write(data []byte) (int, error) {
	return c.w.Write(data)
}

func (c _compressor) Flush() error {
	_flusher, ok := c.w.(flusher)
	if ok {
		return _flusher.Flush()
	}
	return nil
}

func (c _compressor) Close() error {
	closer, ok := c.w.(io.Closer)
	if ok {
		return closer.Close()
	}
	return nil
}

func getCompressor(compressType int, w io.Writer) (Compressor, error) {
	var writer io.Writer
	var err error
	switch compressType {
	case NONE:
		writer = NewNoneCompressWriter(w)
	case BZIP2:
		writer, err = bzip2.NewWriter(w, nil)
	case LZ4:
		writer = lz4.NewWriter(w)
	case BROTLI:
		writer = brotli.NewWriter(w)
	case XZ:
		writer, err = xz.NewWriter(w)
	case GZIP:
		fallthrough
	default:
		//return newFlushWriter(gzip.NewWriter(w)), nil
		writer = gzip.NewWriter(w)
	}
	if err != nil {
		return nil, err
	}
	return _compressor{w: writer}, nil
	//return nil, errors.New(fmt.Sprintf("unknown package compression type %d", compressType))
}

func Compress(data []byte, compressType int) ([]byte, error) {

	buf := &bytes.Buffer{}
	w, err := getCompressor(compressType, buf)

	if nil != err {
		return nil, err
	}
	_, err = w.Write(data)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
