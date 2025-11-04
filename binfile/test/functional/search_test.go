package functional

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/skiloop/binfiles/binfile"
	"github.com/skiloop/binfiles/binfile/test/common"
)

// TestSearchFunctionality 测试搜索功能
func TestSearchFunctionality(t *testing.T) {
	root := common.GetTestDir("test_search")
	os.MkdirAll(root, 0755)
	defer common.CleanupTestDir(root)

	// 创建包含特定键的测试文档
	testDocs := []*binfile.Doc{
		{Key: []byte("test-key-1"), Content: []byte("content1")},
		{Key: []byte("test-key-2"), Content: []byte("content2")},
		{Key: []byte("special-key"), Content: []byte("special-content")},
		{Key: []byte("test-key-3"), Content: []byte("content3")},
	}

	testFile := filepath.Join(root, "test.bin")
	err := common.WriteTestFile(testFile, testDocs, binfile.NONE)
	if err != nil {
		t.Fatalf("Write test file failed: %v", err)
	}

	reader, err := binfile.NewBinReader(testFile, binfile.NONE)
	if err != nil {
		t.Fatalf("NewBinReader failed: %v", err)
	}
	defer reader.Close()

	// 测试搜索功能
	searchTests := []struct {
		pattern     string
		expectedKey string
	}{
		{"^special-key$", "special-key"},
		{"^test-key-2$", "test-key-2"},
		{"test-key", "test-key-1"}, // 应该找到第一个匹配的
	}

	for _, test := range searchTests {
		t.Run(test.pattern, func(t *testing.T) {
			pos := reader.Search(binfile.SearchOption{
				Key:    test.pattern,
				Skip:   0,
				Offset: 0,
			})

			if pos < 0 {
				t.Fatalf("Search failed for pattern: %s", test.pattern)
			}

			doc, err := reader.Read(pos, true)
			if err != nil {
				t.Fatalf("Read found document failed: %v", err)
			}

			if string(doc.Key) != test.expectedKey {
				t.Errorf("Expected key %s, got %s", test.expectedKey, string(doc.Key))
			}
		})
	}
}
