package binfile

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestBinWriterCreation 测试BinWriter创建
func TestBinWriterCreation(t *testing.T) {
	root := GetTestDir("test_new_bin_writer")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	compressTypes := GetAllCompressionTypes()
	for _, compressType := range compressTypes {
		compression := GetCompressionTypeName(compressType)
		t.Run(compression, func(t *testing.T) {
			// 创建测试文件
			tmpFile := filepath.Join(root, fmt.Sprintf("test_%s.bin", compression))
			testDocs := CreateTestDocs(5)

			// 测试 NewBinWriter
			writer := NewBinWriter(tmpFile, compressType)
			err := writer.Open()
			if err != nil {
				t.Fatalf("BinWriter Open failed: %v", err)
			}
			defer func() {
				_ = writer.Close()
			}()

			// 写入测试文档
			for _, doc := range testDocs {
				_, err := writer.Write(doc)
				if err != nil {
					t.Fatalf("Write failed: %v", err)
				}
			}

			// 验证文件已创建
			if _, err := os.Stat(tmpFile); os.IsNotExist(err) {
				t.Fatalf("output file was not created: %s", tmpFile)
			}
		})
	}
}

// TestDocumentReadWrite 测试文档读写
func TestDocumentReadWrite(t *testing.T) {
	root := GetTestDir("test_doc_rw")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	testDocs := []*Doc{
		{Key: []byte("small"), Content: []byte("Hello World")},
		{Key: []byte("medium"), Content: []byte(RandStringBytesMaskImprSrc(1024))},
		{Key: []byte("large"), Content: []byte(RandStringBytesMaskImprSrc(8192))},
		{Key: []byte("binary"), Content: []byte{0x00, 0x01, 0xFF, 0xFE, 0x7F, 0x80}},
	}

	compressTypes := GetAllCompressionTypes()
	for _, compressType := range compressTypes {
		compression := GetCompressionTypeName(compressType)
		t.Run(compression, func(t *testing.T) {
			testFile := filepath.Join(root, fmt.Sprintf("test_%s.bin", compression))

			// 写入文档
			err := WriteTestFile(testFile, testDocs, compressType)
			if err != nil {
				t.Fatalf("Write test file failed: %v", err)
			}

			// 读取并验证文档
			reader, err := NewBinReader(testFile, compressType)
			if err != nil {
				t.Fatalf("NewBinReader failed: %v", err)
			}
			defer reader.Close()

			// 使用Search和Read来验证文档
			for i, expectedDoc := range testDocs {
				pos := reader.Search(SearchOption{
					Key:    fmt.Sprintf("^%s$", string(expectedDoc.Key)),
					Skip:   1,
					Offset: 0,
				})

				if pos < 0 {
					t.Fatalf("Search failed for document %d", i)
				}

				actualDoc, err := reader.Read(pos, true)
				if err != nil || actualDoc == nil {
					t.Fatalf("Read document failed: index %d, %v", i, err)
				}

				if !bytes.Equal(expectedDoc.Key, actualDoc.Key) || !bytes.Equal(expectedDoc.Content, actualDoc.Content) {
					t.Errorf("Document mismatch: index %d\nExpected: %v\nActual: %v",
						i, expectedDoc, actualDoc)
				}
			}
		})
	}
}
