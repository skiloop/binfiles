package binfile

import (
	"bytes"
	"compress/gzip"
	"io"

	"github.com/andybalholm/brotli"
	"github.com/dsnet/compress/bzip2"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
)

type flusher interface {
	Flush() error
}

type Compressor interface {
	flusher
	io.Writer
	io.Closer
	Reset(w io.Writer) error
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

func (c *_compressor) Reset(w io.Writer) error {
	c.w = w
	return nil
}

type gzipCompressor struct {
	gzip.Writer
}

func (c *gzipCompressor) Reset(w io.Writer) error {
	c.Writer.Reset(w)
	return nil
}

type bzip2Compressor struct {
	bzip2.Writer
}

// Flush implements Compressor.
func (c *bzip2Compressor) Flush() error {
	return nil
}

type lz4Compressor struct {
	lz4.Writer
}

func (c *lz4Compressor) Reset(w io.Writer) error {
	c.Writer.Reset(w)
	return nil
}

type brotliCompressor struct {
	brotli.Writer
}

func (c *brotliCompressor) Reset(w io.Writer) error {
	c.Writer.Reset(w)
	return nil
}

type xzCompressor struct {
	xw *xz.Writer
}

// Close implements Compressor.
func (c *xzCompressor) Close() error {
	return c.xw.Close()
}

// Flush implements Compressor.
func (c *xzCompressor) Flush() error {
	return nil
}

// Write implements Compressor.
func (c *xzCompressor) Write(p []byte) (n int, err error) {
	return c.xw.Write(p)
}

func (c *xzCompressor) Reset(w io.Writer) error {
	wt, err := xz.NewWriter(w)
	if err != nil {
		return err
	}
	c.xw = wt
	return nil
}

func getCompressor(compressType int, w io.Writer) (Compressor, error) {
	switch compressType {
	case NONE:
		return &_compressor{w: w}, nil
	case BZIP2:
		writer, err := bzip2.NewWriter(w, nil)
		if err != nil {
			return nil, err
		}
		return &bzip2Compressor{Writer: *writer}, nil
	case LZ4:
		writer := lz4.NewWriter(w)
		return &lz4Compressor{Writer: *writer}, nil
	case BROTLI:
		writer := brotli.NewWriter(w)
		return &brotliCompressor{Writer: *writer}, nil
	case XZ:
		writer, err := xz.NewWriter(w)
		if err != nil {
			return nil, err
		}
		return &xzCompressor{xw: writer}, nil
	case GZIP:
		fallthrough
	default:
		//return newFlushWriter(gzip.NewWriter(w)), nil
		writer := gzip.NewWriter(w)
		return &gzipCompressor{Writer: *writer}, nil
	}

}

func Compress(data []byte, compressType int) ([]byte, error) {
	// 使用内存池优化
	return GlobalMemoryPool.CompressWithPool(data, compressType)
}

// CompressOriginal 原始的压缩实现，保留作为备用
func CompressOriginal(data []byte, compressType int) ([]byte, error) {
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
