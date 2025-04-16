package binfile

import (
	"bytes"
	"compress/gzip"
	"github.com/andybalholm/brotli"
	"github.com/dsnet/compress/bzip2"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
)

func Compress(data []byte, compressType int) ([]byte, error) {

	switch compressType {
	case NONE:
		return data, nil
	case BZIP2:
		return bz2Compress(data)
	case LZ4:
		return bz2Compress(data)
	case BROTLI:
		return brotliCompress(data)
	case XZ:
		return xzCompress(data)
	case GZIP:
		fallthrough
	default:
		return gzipCompress(data)
	}
}

func lz4Compress(data []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w := lz4.NewWriter(buf)
	_, err := w.Write(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func brotliCompress(data []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w := brotli.NewWriter(buf)
	_, err := w.Write(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gzipCompress(data []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w := gzip.NewWriter(buf)
	_, err := w.Write(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func bz2Compress(data []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w, err := bzip2.NewWriter(buf, nil)
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

func xzCompress(data []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w, err := xz.NewWriter(buf)
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
