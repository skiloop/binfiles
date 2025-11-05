package binfile

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestBinReaderCreation 测试BinReader创建
func TestBinReaderCreation(t *testing.T) {
	root := GetTestDir("test_new_bin_reader")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	compressTypes := GetAllCompressionTypes()
	for _, compressType := range compressTypes {
		compression := GetCompressionTypeName(compressType)
		t.Run(compression, func(t *testing.T) {
			// 创建测试文件
			tmpFile := filepath.Join(root, fmt.Sprintf("test_%s.bin", compression))
			testDocs := CreateTestDocs(10)

			err := WriteTestFile(tmpFile, testDocs, compressType)
			if err != nil {
				t.Fatalf("failed to write test file: %v", err)
			}

			// 测试 NewBinReader
			reader, err := NewBinReader(tmpFile, compressType)
			if err != nil {
				t.Fatalf("NewBinReader failed: %v", err)
			}
			defer reader.Close()

			// 测试读取功能
			var dc *Doc
			for i := 0; i < len(testDocs); i++ {
				dc, err = reader.Read(int64(-1), compressType != NONE)
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
