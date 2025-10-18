package binfile

import (
	"bytes"
	"reflect"
	"testing"
)

func TestDecompress(t *testing.T) {
	compressTypes := []int{NONE, GZIP, BROTLI, BZIP2, LZ4, XZ}
	data := []byte(RandStringBytesMaskImprSrc(1024))

	for _, compressType := range compressTypes {
		t.Run(getCompressionTypeName(compressType), func(t *testing.T) {
			compTypeName := getCompressionTypeName(compressType)
			compressed, err := CompressOriginal(data, compressType)
			if err != nil {
				t.Fatalf("compress error: %v", err)
			}
			decompressed, err := Decompress(compressed, compressType)
			if err != nil {
				t.Fatalf("decompress error: %v", err)
			}
			if !bytes.Equal(data, decompressed) {
				t.Fatalf("decompress error: data mismatch")
			}
			t.Logf("%s decompress test success", compTypeName)
		})
	}
}

func TestCompress(t *testing.T) {
	compressTypes := []int{NONE, GZIP, BROTLI, BZIP2, LZ4, XZ}
	// compressTypes := []int{GZIP}
	data := []byte(RandStringBytesMaskImprSrc(1024))

	for _, compressType := range compressTypes {
		t.Run(getCompressionTypeName(compressType), func(t *testing.T) {
			compTypeName := getCompressionTypeName(compressType)
			compressed, err := GlobalMemoryPool.CompressWithPool(data, compressType)
			if err != nil || len(compressed) == 0 {
				t.Fatalf("compress error: %v", err)
			}
			decompressed, err := DecompressOriginal(compressed, compressType)
			if err != nil {
				t.Fatalf("decompress error: %v", err)
			}
			if !bytes.Equal(data, decompressed) {
				t.Fatalf("decompress error: data mismatch")
			}
			t.Logf("%s compress test success", compTypeName)
		})
	}
}

func TestDocCompress(t *testing.T) {
	compressTypes := []int{NONE, GZIP, BROTLI, BZIP2, LZ4, XZ}
	doc := &Doc{
		Key:     []byte("test-key"),
		Content: []byte(RandStringBytesMaskImprSrc(1024)),
	}

	for _, compressType := range compressTypes {
		compTypeName := getCompressionTypeName(compressType)

		t.Run(compTypeName, func(t *testing.T) {
			compressed, err := CompressDoc(doc, compressType)
			if err != nil {
				t.Fatalf("compress error: %v", err)
			}
			decompressed, err := DecompressDoc(compressed, compressType, false)
			if err != nil {
				t.Fatalf("decompress error: %v", err)
			}
			if !reflect.DeepEqual(*doc, *decompressed) {
				t.Fatalf("decompress error: data mismatch")
			}
			t.Logf("%s doc compress test success", compTypeName)
		})
	}
}
