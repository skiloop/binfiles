package unit

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/skiloop/binfiles/binfile"
	"github.com/skiloop/binfiles/binfile/test/common"
)

// TestBinReaderCreation 测试BinReader创建
func TestBinReaderCreation(t *testing.T) {
	root := common.GetTestDir("test_new_bin_reader")
	os.MkdirAll(root, 0755)
	defer common.CleanupTestDir(root)

	compressTypes := common.GetAllCompressionTypes()
	for _, compressType := range compressTypes {
		compression := common.GetCompressionTypeName(compressType)
		t.Run(compression, func(t *testing.T) {
			// 创建测试文件
			tmpFile := filepath.Join(root, fmt.Sprintf("test_%s.bin", compression))
			testDocs := common.CreateTestDocs(10)

			err := common.WriteTestFile(tmpFile, testDocs, compressType)
			if err != nil {
				t.Fatalf("failed to write test file: %v", err)
			}

			// 测试 NewBinReader
			reader, err := binfile.NewBinReader(tmpFile, compressType)
			if err != nil {
				t.Fatalf("NewBinReader failed: %v", err)
			}
			defer reader.Close()

			// 测试读取功能
			var dc *binfile.Doc
			for i := 0; i < len(testDocs); i++ {
				dc, err = reader.Read(int64(-1), compressType != binfile.NONE)
				if err != nil {
					t.Fatalf("failed to read from BinReader: %v", err)
				}
			}

			if dc == nil {
				t.Fatalf("expected non-nil Doc")
			}

			// 验证最后一个文档的内容
			lastDoc := testDocs[len(testDocs)-1]
			if string(dc.Key) != string(lastDoc.Key) {
				t.Fatalf("expected %s, got %s", string(lastDoc.Key), string(dc.Key))
			}
			if string(dc.Content) != string(lastDoc.Content) {
				t.Fatalf("expected %s, got %s", string(lastDoc.Content), string(dc.Content))
			}
		})
	}
}

// TestBinWriterCreation 测试BinWriter创建
func TestBinWriterCreation(t *testing.T) {
	root := common.GetTestDir("test_new_bin_writer")
	os.MkdirAll(root, 0755)
	defer common.CleanupTestDir(root)

	compressTypes := common.GetAllCompressionTypes()
	for _, compressType := range compressTypes {
		compression := common.GetCompressionTypeName(compressType)
		t.Run(compression, func(t *testing.T) {
			// 创建测试文件
			tmpFile := filepath.Join(root, fmt.Sprintf("test_%s.bin", compression))
			testDocs := common.CreateTestDocs(5)

			// 测试 NewBinWriter
			writer := binfile.NewBinWriter(tmpFile, compressType)
			err := writer.Open()
			if err != nil {
				t.Fatalf("BinWriter Open failed: %v", err)
			}
			defer writer.Close()

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
	root := common.GetTestDir("test_doc_rw")
	os.MkdirAll(root, 0755)
	defer common.CleanupTestDir(root)

	testDocs := []*binfile.Doc{
		{Key: []byte("small"), Content: []byte("Hello World")},
		{Key: []byte("medium"), Content: []byte(common.RandStringBytesMaskImprSrc(1024))},
		{Key: []byte("large"), Content: []byte(common.RandStringBytesMaskImprSrc(8192))},
		{Key: []byte("binary"), Content: []byte{0x00, 0x01, 0xFF, 0xFE, 0x7F, 0x80}},
	}

	compressTypes := common.GetAllCompressionTypes()
	for _, compressType := range compressTypes {
		compression := common.GetCompressionTypeName(compressType)
		t.Run(compression, func(t *testing.T) {
			testFile := filepath.Join(root, fmt.Sprintf("test_%s.bin", compression))

			// 写入文档
			err := common.WriteTestFile(testFile, testDocs, compressType)
			if err != nil {
				t.Fatalf("Write test file failed: %v", err)
			}

			// 读取并验证文档
			reader, err := binfile.NewBinReader(testFile, compressType)
			if err != nil {
				t.Fatalf("NewBinReader failed: %v", err)
			}
			defer reader.Close()

			// 使用Search和Read来验证文档
			for i, expectedDoc := range testDocs {
				pos := reader.Search(binfile.SearchOption{
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
