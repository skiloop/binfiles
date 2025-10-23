package functional

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/skiloop/binfiles/binfile"
	"github.com/skiloop/binfiles/binfile/test/common"
)

// TestRepackFunctionality 测试repack功能
func TestRepackFunctionality(t *testing.T) {
	binfile.RedirectToDevNull()
	outputRoot := common.GetTestDir("repack_functional_test")
	testFile := filepath.Join(outputRoot, "test.bin")
	os.MkdirAll(outputRoot, 0755)
	defer common.CleanupTestDir(outputRoot)

	// 创建测试文件
	testDocs := common.CreateTestDocs(1000)
	err := common.WriteTestFile(testFile, testDocs, binfile.NONE)
	if err != nil {
		t.Fatalf("Create test file failed: %v", err)
	}

	testConfigs := []struct {
		mode         string
		compressType int
		workers      int
	}{
		{"doc", binfile.XZ, 4},
		{"file", binfile.XZ, 4},
		{"doc", binfile.GZIP, 4},
		{"file", binfile.GZIP, 4},
		{"doc", binfile.BZIP2, 4},
		{"file", binfile.BZIP2, 4},
		{"doc", binfile.BROTLI, 4},
		{"file", binfile.BROTLI, 4},
		{"doc", binfile.LZ4, 4},
		{"file", binfile.LZ4, 4},
	}

	for _, config := range testConfigs {
		compTypeName := common.GetCompressionTypeName(config.compressType)
		t.Run(fmt.Sprintf("Repack_%s_%s_%d", config.mode, compTypeName, config.workers), func(t *testing.T) {
			outputFile := filepath.Join(outputRoot, fmt.Sprintf("repack_%s_%s_%d.bin", config.mode, compTypeName, config.workers))

			opt := binfile.RepackCmd{
				Source:              testFile,
				Target:              outputFile,
				Workers:             config.workers,
				Mode:                config.mode,
				SourceCompressType:  "none",
				TargetCompressType:  compTypeName,
				PackageCompressType: "none",
				Limit:               0,
			}

			err := binfile.Repack(opt)
			if err != nil {
				t.Fatalf("Repack failed: %v", err)
			}

			// 验证输出文件
			stat, err := os.Stat(outputFile)
			if err != nil {
				t.Fatalf("Get output file stat failed: %v", err)
			}

			if stat.Size() == 0 {
				t.Fatalf("Output file size is 0: %s", outputFile)
			}

			// 验证可以读取repack后的文件
			br, err := binfile.NewBinReader(outputFile, config.compressType)
			if err != nil {
				t.Fatalf("Read repacked file failed: %v", err)
			}
			defer br.Close()

			// 搜索第一个文档
			pos := br.Search(binfile.SearchOption{Key: "^test-key-0$", Skip: 1, Offset: 0})
			if pos < 0 {
				t.Fatalf("Document not found: test-key-0, pos: %d", pos)
			}

			rdoc, err := br.Read(pos, true)
			if err != nil {
				t.Fatalf("Read document failed: %v", err)
			}

			if !reflect.DeepEqual(testDocs[0], rdoc) {
				t.Fatalf("Document mismatch: %v != %v", testDocs[0], rdoc)
			}
		})
	}
}

// TestLargeFileHandling 测试大文件处理
func TestLargeFileHandling(t *testing.T) {
	binfile.RedirectToDevNull()
	outputRoot := common.GetTestDir("large_file_test")
	os.MkdirAll(outputRoot, 0755)
	defer common.CleanupTestDir(outputRoot)

	// 创建较大的测试文件
	testDocs := common.CreateTestDocs(1000)
	testFile := filepath.Join(outputRoot, "large_test.bin")

	err := common.WriteTestFile(testFile, testDocs, binfile.NONE)
	if err != nil {
		t.Fatalf("Create large test file failed: %v", err)
	}

	// 测试读取大文件
	reader, err := binfile.NewBinReader(testFile, binfile.NONE)
	if err != nil {
		t.Fatalf("NewBinReader failed: %v", err)
	}
	defer reader.Close()
	stat, err := os.Stat(testFile)
	if err != nil {
		t.Fatalf("Get file stat failed: %v", err)
	}
	binfile.LogInfo("file size: %d\n", stat.Size())
	// 测试计数功能
	binfile.Verbose = true
	binfile.SetGlobalLogLevel(binfile.DEBUG)
	count := reader.Count(&binfile.CountOption{
		Offset:      0,
		End:         -1,
		WorkerCount: 1,
		KeyOnly:     false,
		VerboseStep: 1,
		SkipError:   true,
	})
	binfile.Verbose = false
	binfile.SetGlobalLogLevel(binfile.INFO)
	if count != int64(len(testDocs)) {
		t.Errorf("Expected count %d, got %d", len(testDocs), count)
	}

	// 测试搜索功能
	pos := reader.Search(binfile.SearchOption{
		Key:    "^test-key-500$",
		Skip:   1,
		Offset: 0,
	})

	if pos < 0 {
		t.Fatalf("Search failed for test-key-500: expect pos >=0, but got %d", pos)
	}

	doc, err := reader.Read(pos, true)
	if err != nil {
		t.Fatalf("Read document failed: %v", err)
	}

	if string(doc.Key) != "test-key-500" {
		t.Errorf("Expected key test-key-500, got %s", string(doc.Key))
	}
}

// TestConcurrentAccess 测试并发访问
func TestConcurrentAccess(t *testing.T) {
	binfile.RedirectToDevNull()
	outputRoot := common.GetTestDir("concurrent_test")
	os.MkdirAll(outputRoot, 0755)
	defer common.CleanupTestDir(outputRoot)

	// 创建测试文件
	testDocs := common.CreateTestDocs(100)
	testFile := filepath.Join(outputRoot, "concurrent_test.bin")

	err := common.WriteTestFile(testFile, testDocs, binfile.NONE)
	if err != nil {
		t.Fatalf("Create test file failed: %v", err)
	}

	const goroutines = 5
	const iterations = 20

	done := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			reader, err := binfile.NewBinReader(testFile, binfile.NONE)
			if err != nil {
				t.Errorf("Goroutine %d: NewBinReader failed: %v", id, err)
				return
			}
			defer reader.Close()

			for j := 0; j < iterations; j++ {
				// 随机搜索一个文档
				keyPattern := fmt.Sprintf("^test-key-%d$", j%len(testDocs))
				pos := reader.Search(binfile.SearchOption{
					Key:    keyPattern,
					Skip:   1,
					Offset: 0,
				})

				if pos < 0 {
					t.Errorf("Goroutine %d, iteration %d: Search failed", id, j)
					continue
				}

				doc, err := reader.Read(pos, true)
				if err != nil {
					t.Errorf("Goroutine %d, iteration %d: Read failed: %v", id, j, err)
					continue
				}

				expectedKey := fmt.Sprintf("test-key-%d", j%len(testDocs))
				if string(doc.Key) != expectedKey {
					t.Errorf("Goroutine %d, iteration %d: Expected key %s, got %s",
						id, j, expectedKey, string(doc.Key))
				}
			}
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < goroutines; i++ {
		<-done
	}
}

// TestErrorRecovery 测试错误恢复
func TestErrorRecovery(t *testing.T) {
	binfile.RedirectToDevNull()
	outputRoot := common.GetTestDir("error_recovery_test")
	os.MkdirAll(outputRoot, 0755)
	defer common.CleanupTestDir(outputRoot)

	// 创建测试文件
	testDocs := common.CreateTestDocs(50)
	testFile := filepath.Join(outputRoot, "error_test.bin")

	err := common.WriteTestFile(testFile, testDocs, binfile.NONE)
	if err != nil {
		t.Fatalf("Create test file failed: %v", err)
	}

	reader, err := binfile.NewBinReader(testFile, binfile.NONE)
	if err != nil {
		t.Fatalf("NewBinReader failed: %v", err)
	}
	defer reader.Close()

	// 测试无效偏移量
	_, err = reader.Read(-1, true)
	if err != nil {
		t.Error("Unexpected error for negative offset:", err)
	}

	// 测试超出范围的偏移量
	_, err = reader.Read(999999999, true)
	if err == nil {
		t.Error("Expected error for out-of-range offset")
	}

	// 测试搜索不存在的键
	pos := reader.Search(binfile.SearchOption{
		Key:    "^nonexistent-key$",
		Skip:   1,
		Offset: 0,
	})

	if pos >= 0 {
		t.Error("Expected negative position for nonexistent key")
	}
}

// TestCompressionTypes 测试所有压缩类型的功能
func TestCompressionTypes(t *testing.T) {
	binfile.RedirectToDevNull()
	outputRoot := common.GetTestDir("compression_types_test")
	os.MkdirAll(outputRoot, 0755)
	defer common.CleanupTestDir(outputRoot)

	testDocs := []*binfile.Doc{
		{Key: []byte("small"), Content: []byte("Hello World")},
		{Key: []byte("medium"), Content: []byte(common.RandStringBytesMaskImprSrc(1024))},
		{Key: []byte("large"), Content: []byte(common.RandStringBytesMaskImprSrc(8192))},
		{Key: []byte("binary"), Content: []byte{0x00, 0x01, 0xFF, 0xFE, 0x7F, 0x80}},
	}

	compressTypes := common.GetAllCompressionTypes()

	for _, compressType := range compressTypes {
		compTypeName := common.GetCompressionTypeName(compressType)
		t.Run(compTypeName, func(t *testing.T) {
			testFile := filepath.Join(outputRoot, fmt.Sprintf("test_%s.bin", compTypeName))

			// 写入文档
			err := common.WriteTestFile(testFile, testDocs, compressType)
			if err != nil {
				t.Fatalf("Write test file failed: %v", err)
			}

			// 读取并验证
			reader, err := binfile.NewBinReader(testFile, compressType)
			if err != nil {
				t.Fatalf("NewBinReader failed: %v", err)
			}
			defer reader.Close()

			// 验证所有文档都可以正确读取
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
				if err != nil {
					t.Fatalf("Read document %d failed: %v", i, err)
				}

				if !reflect.DeepEqual(expectedDoc, actualDoc) {
					t.Errorf("Document %d mismatch: %v != %v", i, expectedDoc, actualDoc)
				}
			}
		})
	}
}
