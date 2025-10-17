package binfile

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

type DocCompressor interface {
	CompressDoc(doc *Doc, compressType int) (dst *Doc, err error)
	Decompress(doc *Doc, compressType int, verbose bool) (dst *Doc, err error)
}

type OptimizedDocCompressor struct {
}
type oldCompressor struct {
}

func (c OptimizedDocCompressor) Decompress(doc *Doc, compressType int, verbose bool) (dst *Doc, err error) {
	if NONE == compressType {
		return doc, nil
	}
	data, err := GlobalMemoryPool.DecompressWithPool(doc.Content, compressType)
	if err != nil {
		if verbose {
			_, _ = fmt.Fprintf(os.Stderr, "decompress error: %v\n", err)
		}
		return nil, err
	}
	return &Doc{Key: CloneBytes(doc.Key), Content: data}, nil
}

func (c OptimizedDocCompressor) CompressDoc(doc *Doc, compressType int) (dst *Doc, err error) {
	if NONE == compressType {
		return doc, nil
	}
	buf, err := GlobalMemoryPool.CompressWithPool(doc.Content, compressType)
	if err != nil {
		return nil, err
	}
	return &Doc{Key: CloneBytes(doc.Key), Content: buf}, nil
}

// CompressDoc 原始的文档压缩实现，保留作为备用
func (c oldCompressor) CompressDoc(doc *Doc, compressType int) (dst *Doc, err error) {
	if NONE == compressType {
		return doc, nil
	}
	buf, err := CompressOriginal(doc.Content, compressType)
	if err != nil {
		return nil, err
	}
	return &Doc{Key: CloneBytes(doc.Key), Content: buf}, nil
}

func (c oldCompressor) Decompress(doc *Doc, compressType int, verbose bool) (dst *Doc, err error) {
	if NONE == compressType {
		return doc, nil
	}
	reader, err := getDecompressor(compressType, bytes.NewReader(doc.Content))
	if err != nil {
		if Verbose {
			_, _ = fmt.Fprintf(os.Stderr, "decompressor reader creation error: %v\n", err)
		}
		return nil, ErrDecompressReader
	}

	var data []byte
	data, err = io.ReadAll(reader)
	if err != nil {
		if verbose {
			_, _ = fmt.Fprintf(os.Stderr, "decompress error: %v\n", err)
		}
		return nil, ErrValueDecompress
	}
	return &Doc{Key: CloneBytes(doc.Key), Content: data}, nil
}

func DecompressDoc(doc *Doc, compressType int, verbose bool) (dst *Doc, err error) {
	if NONE == compressType {
		return doc, nil
	}
	return GlobalMemoryPool.DecompressDocWithPool(doc, compressType)
}

func CompressDoc(doc *Doc, compressType int) (dst *Doc, err error) {
	if NONE == compressType {
		return doc, nil
	}
	return GlobalMemoryPool.CompressDocWithPool(doc, compressType)
}
