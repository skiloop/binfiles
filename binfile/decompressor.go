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

// Decompressor 解压缩器接口
type Decompressor interface {
	io.Reader
	io.Closer
	Reset(src io.Reader) error
}

type noneDecompressor struct {
	src io.Reader
}

func (d *noneDecompressor) Read(p []byte) (int, error) {
	return d.src.Read(p)
}

func (d *noneDecompressor) Close() error {
	return nil
}

func (d *noneDecompressor) Reset(src io.Reader) error {
	d.src = src
	return nil
}

func NewNoneDecompressor(src io.Reader) Decompressor {
	return &noneDecompressor{src: src}
}

type lz4Decompressor struct {
	lr *lz4.Reader
}

func (d *lz4Decompressor) Read(p []byte) (int, error) {
	return d.lr.Read(p)
}

func (d *lz4Decompressor) Close() error {
	return nil
}

func (d *lz4Decompressor) Reset(src io.Reader) error {
	d.lr = lz4.NewReader(src)
	return nil
}

func NewLz4Decompressor(src io.Reader) Decompressor {
	return &lz4Decompressor{lr: lz4.NewReader(src)}
}

type brotliDecompressor struct {
	br *brotli.Reader
}

// Close implements Decompressor.
func (d *brotliDecompressor) Close() error {
	return nil
}

// Read implements Decompressor.
func (d *brotliDecompressor) Read(p []byte) (n int, err error) {
	return d.br.Read(p)
}

func (d *brotliDecompressor) Reset(src io.Reader) error {
	if d.br == nil {
		d.br = brotli.NewReader(src)
		return nil
	}
	return d.br.Reset(src)
}

type xzDecompressor struct {
	xr *xz.Reader
}

// Close implements Decompressor.
func (d *xzDecompressor) Close() error {
	return nil
}

// Read implements Decompressor.
func (d *xzDecompressor) Read(p []byte) (n int, err error) {
	return d.xr.Read(p)
}

func (d *xzDecompressor) Reset(src io.Reader) error {
	xr, err := xz.NewReader(src)
	if err != nil {
		return err
	}
	d.xr = xr
	return nil
}

type bzip2Decompressor struct {
	br *bzip2.Reader
}

func (d *bzip2Decompressor) Read(p []byte) (int, error) {
	return d.br.Read(p)
}

func (d *bzip2Decompressor) Close() error {
	if d.br != nil {
		return d.br.Close()
	}
	return nil
}

func (d *bzip2Decompressor) Reset(src io.Reader) error {
	if d.br == nil {
		br, err := bzip2.NewReader(src, nil)
		if err != nil {
			return err
		}
		d.br = br
		return nil
	}
	return d.br.Reset(src)
}

type gzipDecompressor struct {
	gr *gzip.Reader
}

func (d *gzipDecompressor) Read(p []byte) (int, error) {
	return d.gr.Read(p)
}

func (d *gzipDecompressor) Close() error {
	if d.gr != nil {
		return d.gr.Close()
	}
	return nil
}

func (d *gzipDecompressor) Reset(src io.Reader) error {
	if d.gr == nil {
		gr, err := gzip.NewReader(src)
		if err != nil {
			return err
		}
		d.gr = gr
		return nil
	}
	return d.gr.Reset(src)
}

func getDecompressor(ct int, src io.Reader) (Decompressor, error) {
	switch ct {
	case NONE:
		return NewNoneDecompressor(src), nil
	case LZ4:
		return NewLz4Decompressor(src), nil
	case BROTLI:
		br := brotli.NewReader(src)
		return &brotliDecompressor{br: br}, nil
	case XZ:
		if src == nil {
			return &xzDecompressor{xr: nil}, nil
		}
		xr, err := xz.NewReader(src)
		if err != nil {
			return nil, err
		}
		return &xzDecompressor{xr: xr}, nil
	case BZIP2:
		br, err := bzip2.NewReader(src, nil)
		if err != nil {
			return nil, err
		}
		return &bzip2Decompressor{br: br}, nil
	case GZIP:
		fallthrough
	default:
		if src == nil {
			return &gzipDecompressor{gr: nil}, nil
		}
		gr, err := gzip.NewReader(src)
		if err != nil {
			return nil, err
		}
		return &gzipDecompressor{gr: gr}, nil
	}
}

// Decompress 使用内存池进行数据解压缩
func Decompress(data []byte, compressType int) ([]byte, error) {
	return GlobalMemoryPool.DecompressWithPool(data, compressType)
}

// DecompressOriginal 原始的解压缩实现，保留作为备用
func DecompressOriginal(data []byte, compressType int) ([]byte, error) {
	br := bytes.NewReader(data)
	decompressor, err := getDecompressor(compressType, br)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(decompressor)
}
