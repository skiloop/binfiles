package binfile

import (
	"bytes"
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
