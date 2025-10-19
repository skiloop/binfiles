package binfile

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func createRandDoc() *Doc {
	return &Doc{Key: []byte(GenerateRandomString(10)), Content: []byte(GenerateRandomString(100))}
}

func writeRandDoc(filename string, count, ct int) (doc *Doc, err error) {
	output, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	defer output.Close()
	bw := compressDocWriter{
		docWriter:     docWriter{w: output},
		compressType:  ct,
		docCompressor: OptimizedDocCompressor{},
	}
	for i := 0; i < count; i++ {
		doc = createRandDoc()
		_, err = bw.Write(doc)
		if err != nil {
			return nil, err
		}
	}
	return doc, nil
}

func TestNewBinReader(t *testing.T) {
	root := getTestDir("test_new_bin_reader")
	os.MkdirAll(root, 0755)
	defer os.RemoveAll(root)
	compressTypes := []int{GZIP, BZIP2, BROTLI, LZ4, XZ, NONE}
	for _, compressType := range compressTypes {
		compression := getCompressionTypeName(compressType)
		t.Run(compression, func(t *testing.T) {
			// 创建一个临时文件用于测试
			tmpFile := filepath.Join(root, fmt.Sprintf("test_%s.bin", compression))

			// 写入一些测试数据
			count := 10
			doc, err := writeRandDoc(tmpFile, count, compressType)
			if err != nil {
				t.Fatalf("failed to write to temp file: %v", err)
			}
			// 测试 NewBinReader
			reader, err := NewBinReader(tmpFile, compressType)
			if err != nil {
				t.Fatalf("NewBinReader failed: %v", err)
			}
			defer reader.Close()

			// 验证返回的 BinReader 实例
			r, ok := reader.(*binReader)
			if !ok {
				t.Fatalf("expected non-nil BinReader")
			}
			var dc *Doc
			offset := int64(0)
			for i := 0; i < count; i++ {
				dc, err = r.Read(offset, true)
				if err != nil {
					t.Fatalf("failed to read from BinReader: %v", err)
				}
				offset, _ = r.file.Seek(int64(0), io.SeekCurrent)
			}
			if dc == nil {
				t.Fatalf("expected non-nil Doc")
			}
			if string(dc.Key) != string(doc.Key) {
				t.Fatalf("expected %s, got %s", string(doc.Key), string(dc.Key))
			}
			if string(dc.Content) != string(doc.Content) {
				t.Fatalf("expected %s, got %s", string(doc.Content), string(dc.Content))
			}
		})
	}
}

// func TestReadDocs(t *testing.T) {
// 	root := getTestDir("test_read_docs")
// 	os.MkdirAll(root, 0755)
// 	defer os.RemoveAll(root)
// 	compressTypes := []int{GZIP, BZIP2, BROTLI, LZ4, XZ, NONE}
// 	for _, compressType := range compressTypes {
// 		compression := getCompressionTypeName(compressType)
// 		t.Run(compression, func(t *testing.T) {
// 			// 创建一个临时文件用于测试
// 			tmpFile := filepath.Join(root, fmt.Sprintf("test_%s.bin", compression))

// 			// 写入一些测试数据
// 			count := 20
// 			_, err := writeRandDoc(tmpFile, count, compressType)
// 			if err != nil {
// 				t.Fatalf("failed to write to temp file: %v", err)
// 			}

// 			// 初始化 BinReader
// 			reader, err := NewBinReader(tmpFile, compressType)
// 			if err != nil {
// 				t.Fatalf("NewBinReader failed: %v", err)
// 			}
// 			defer reader.Close()

// 			// 测试 ReadDocs
// 			opt := &ReadOption{
// 				Offset:    0,
// 				Limit:     int32(count),
// 				Step:      1,
// 				Output:    "",
// 				SkipError: true,
// 			}
// 			reader.ReadDocs(opt)
// 		})
// 	}
// }
