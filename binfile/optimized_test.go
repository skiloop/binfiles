package binfile

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const (
	// 定义字符集长度
	charSetLen = int64(len(charset))
	// 63位随机数能包含多少个字符索引 ( log2(charSetLen) )
	// 64个字符，需要 6 bit (2^6 = 64)
	idxBits = 6
	// 定义一个掩码 (6 bit 全部为 1)
	idxMask = 1<<idxBits - 1
	// 一个 Int63 能容纳的字符数量 (63 / 6 = 10)
	idxMax = 63 / idxBits
)

// 创建一个全局的、已播种的随机源
var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()),
)

// RandStringBytesMaskImprSrc 快速生成指定长度的随机字符串
func RandStringBytesMaskImprSrc(n int) string {
	b := make([]byte, n)
	// i: 字节切片索引
	// cache: 缓存的 63 位随机数
	// remain: 缓存中还剩余多少个字符索引可用
	for i, cache, remain := n-1, seededRand.Int63(), idxMax; i >= 0; {
		if remain == 0 {
			// 缓存用尽，重新获取 63 位随机数
			cache, remain = seededRand.Int63(), idxMax
		}

		// 使用掩码 idxMask 获取一个 6 位的索引
		if idx := int(cache & idxMask); idx < int(charSetLen) {
			// 如果索引在字符集范围内，则使用它
			b[i] = charset[idx]
			i--
		}

		// 移动缓存，准备获取下一个索引
		cache >>= idxBits
		remain--
	}

	return string(b)
}

func initTestFile(testFile string, t *testing.T) (picked int, doc *Doc) {
	// 初始化测试文件
	bw := NewBinWriter(testFile, NONE)
	err := bw.Open()
	if err != nil {
		t.Fatalf("打开写入器失败: %v", err)
	}
	total := 1000
	picked = rand.Intn(total)
	i := 0
	for i < total {
		content := RandStringBytesMaskImprSrc(2048)
		_doc := &Doc{Key: []byte(fmt.Sprintf("test%d", i)), Content: []byte(content)}
		_, err = bw.Write(_doc)
		if err != nil {
			t.Fatalf("写入文档失败: %v", err)
		}
		if i == picked {
			doc = _doc
		}
		i++
	}
	err = bw.Close()
	if err != nil {
		t.Fatalf("关闭写入器失败: %v", err)
	}
	LogInfo("initTestFile done with %d documents\n", i)
	return picked, doc
}

// TestOptimizedRepackFunctionality 测试优化后repack的功能
func TestOptimizedRepackFunctionality(t *testing.T) {
	outputRoot := "/tmp/optimized_repack_test"
	testFile := fmt.Sprintf("%s/test.bin", outputRoot)
	os.MkdirAll(outputRoot, 0755)
	defer os.RemoveAll(outputRoot)
	// 初始化测试文件
	picked, doc := initTestFile(testFile, t)
	if doc == nil {
		t.Fatalf("初始化测试文件失败")
	}
	repackModes := []string{"doc", "file"}
	for _, mode := range repackModes {
		t.Run(fmt.Sprintf("OptimizedRepack_%s", mode), func(t *testing.T) {
			outputFile := fmt.Sprintf("%s/optimized_%s.bin", outputRoot, mode)
			opt := RepackCmd{
				Source:              testFile,
				Target:              outputFile,
				Workers:             2,
				Mode:                mode,
				SourceCompressType:  "none",
				TargetCompressType:  "gzip",
				PackageCompressType: "none",
				Limit:               0,
			}
			err := Repack(opt)
			if err != nil {
				t.Fatalf("优化repack失败: %v", err)
			}
			stat, err := os.Stat(outputFile)
			if err != nil {
				t.Fatalf("获取输出文件状态失败: %v", err)
			}
			fileSize := stat.Size()
			if fileSize == 0 {
				t.Fatalf("输出文件大小为0: %s", outputFile)
			}
			t.Logf("输出文件大小: %d", fileSize)
			br, err := NewBinReader(outputFile, GZIP)
			if err != nil {
				t.Fatalf("读取输出文件失败: %v", err)
			}
			pos := br.Search(SearchOption{Key: fmt.Sprintf("^test%d$", picked), Number: 1, Offset: 0})
			if pos < 0 {
				t.Fatalf("文档不存在: %d, pos: %d", picked, pos)
			}
			rdoc, err := br.Read(pos, true)
			if err != nil {
				t.Fatalf("读取文档失败: %v", err)
			}
			if !reflect.DeepEqual(doc, rdoc) {
				t.Fatalf("文档不一致: %v != %v", doc, rdoc)
			}
			t.Logf("优化repack成功，输出文件: %s", outputFile)
		})
	}

}

// TestOptimizedReadWriteAllCompressionTypes 测试所有压缩类型的读写优化
func TestOptimizedReadWriteAllCompressionTypes(t *testing.T) {
	outputRoot := "/tmp/optimized_rw_test"
	os.MkdirAll(outputRoot, 0755)
	defer os.RemoveAll(outputRoot)

	// 定义所有压缩类型
	compressionTypes := []struct {
		name string
		code int
	}{
		{"none", NONE},
		{"gzip", GZIP},
		{"brotli", BROTLI},
		{"bzip2", BZIP2},
		{"lz4", LZ4},
		{"xz", XZ},
	}

	// 测试数据
	testDocs := []*Doc{
		{Key: []byte("small"), Content: []byte("Hello World")},
		{Key: []byte("medium"), Content: []byte(RandStringBytesMaskImprSrc(1024))},
		{Key: []byte("large"), Content: []byte(RandStringBytesMaskImprSrc(8192))},
		{Key: []byte("binary"), Content: []byte{0x00, 0x01, 0xFF, 0xFE, 0x7F, 0x80}},
	}

	for _, compType := range compressionTypes {
		t.Run(fmt.Sprintf("Compression_%s", compType.name), func(t *testing.T) {
			// 测试写入
			testFile := fmt.Sprintf("%s/test_%s.bin", outputRoot, compType.name)

			// 测试原始写入
			t.Run("Write_Original", func(t *testing.T) {
				bw := NewBinWriter(testFile, compType.code)
				err := bw.Open()
				if err != nil {
					t.Fatalf("打开写入器失败: %v", err)
				}
				defer bw.Close()

				for _, doc := range testDocs {
					_, err := bw.Write(doc)
					if err != nil {
						t.Fatalf("写入文档失败: %v", err)
					}
				}
				t.Logf("原始写入成功: %s", compType.name)
			})

			// 测试读取
			t.Run("Read_Verification", func(t *testing.T) {
				br, err := NewBinReader(testFile, compType.code)
				if err != nil {
					t.Fatalf("打开读取器失败: %v", err)
				}
				reader, _ := br.(*binReader)
				defer br.Close()
				pos := int64(0)
				for i, expectedDoc := range testDocs {
					actualDoc, err := reader.docSeeker.ReadAt(pos, true)
					if err != nil || actualDoc == nil {
						t.Fatalf("读取文档失败: index %d, %v", i, err)
					}
					pos, err = reader.docSeeker.Seek(0, io.SeekCurrent)
					if err != nil {
						t.Fatalf("获取当前位置失败: %v", err)
					}
					if !reflect.DeepEqual(expectedDoc, actualDoc) {
						t.Errorf("文档不匹配: index %d\nExpected: %v\nActual: %v",
							i, expectedDoc, actualDoc)
					}
				}
				t.Logf("读取验证成功: %s", compType.name)
			})

			// 测试压缩/解压缩优化
			t.Run("CompressDecompress_Optimized", func(t *testing.T) {
				for _, doc := range testDocs {
					// 测试压缩
					compressed, err := Compress(doc.Content, compType.code)
					if err != nil {
						t.Fatalf("压缩失败: %v", err)
					}

					// 测试解压缩
					if compType.code != NONE {
						decompressed, err := GlobalMemoryPool.DecompressWithPool(compressed, compType.code)
						if err != nil {
							t.Fatalf("解压缩失败: %v", err)
						}

						if !reflect.DeepEqual(doc.Content, decompressed) {
							t.Errorf("解压缩数据不匹配: %s", compType.name)
						}
					} else {
						// NONE类型不需要解压缩
						if !reflect.DeepEqual(doc.Content, compressed) {
							t.Errorf("NONE类型数据不匹配: %s", compType.name)
						}
					}
				}
				t.Logf("压缩/解压缩优化测试成功: %s", compType.name)
			})
		})
	}
}

// TestOptimizedVsOriginalPerformance 测试优化版本与原始版本的性能对比
func TestOptimizedVsOriginalPerformance(t *testing.T) {
	outputRoot := "/tmp/performance_test"
	os.MkdirAll(outputRoot, 0755)
	defer os.RemoveAll(outputRoot)

	// 创建测试数据
	testData := []byte(RandStringBytesMaskImprSrc(4096))
	compressionTypes := []int{NONE, GZIP, BROTLI, BZIP2, LZ4, XZ}
	tests := []struct {
		testType     string
		name         string
		compressFunc func([]byte, int) ([]byte, error)
	}{
		{testType: "Original", name: "原始压缩", compressFunc: CompressOriginal},
		{testType: "Optimized", name: "优化压缩", compressFunc: Compress},
	}

	for _, compType := range compressionTypes {
		compTypeName := getCompressionTypeName(compType)
		t.Run(fmt.Sprintf("Compression_%s", compTypeName), func(t *testing.T) {

			// 测试方法
			for _, test := range tests {
				t.Run(test.testType, func(t *testing.T) {
					start := time.Now()
					var m1, m2 runtime.MemStats
					runtime.GC()
					runtime.ReadMemStats(&m1)

					result, err := test.compressFunc(testData, compType)
					if err != nil {
						t.Fatalf("%s压缩失败: %v", test.name, err)
					}

					duration := time.Since(start)
					runtime.GC()
					runtime.ReadMemStats(&m2)

					t.Logf("%s - %s:", test.name, compTypeName)
					t.Logf("  时间: %v", duration)
					t.Logf("  总分配次数: %d", m2.Mallocs-m1.Mallocs)
					t.Logf("  总释放次数: %d", m2.Frees-m1.Frees)
					t.Logf("  当前堆大小: %d KB", m2.HeapAlloc/1024)
					t.Logf("  GC次数: %d", m2.NumGC-m1.NumGC)
					t.Logf("  压缩后大小: %d bytes", len(result))
				})
			}

		})
	}
}

// TestMemoryPoolConcurrency 测试内存池的并发安全性
func TestMemoryPoolConcurrency(t *testing.T) {
	t.Run("ConcurrentCompression", func(t *testing.T) {
		const goroutines = 10
		const iterations = 100

		done := make(chan bool, goroutines)
		testData := []byte(RandStringBytesMaskImprSrc(1024))
		compTypes := []int{GZIP, BROTLI, BZIP2, LZ4, XZ}

		for i := 0; i < goroutines; i++ {
			go func(id int) {
				defer func() { done <- true }()

				for j := 0; j < iterations; j++ {
					// 测试不同的压缩类型
					compType := compTypes[j%len(compTypes)]

					result, err := Compress(testData, compType)
					if err != nil {
						t.Errorf("Goroutine %d, iteration %d 压缩失败: %v", id, j, err)
						return
					}

					if len(result) == 0 {
						t.Errorf("Goroutine %d, iteration %d 压缩结果为空", id, j)
						return
					}
				}
			}(i)
		}

		// 等待所有goroutine完成
		for i := 0; i < goroutines; i++ {
			<-done
		}

		t.Log("并发压缩测试完成")
	})
}

// TestDocumentCompressionOptimization 测试文档压缩优化
func TestDocumentCompressionOptimization(t *testing.T) {
	outputRoot := "/tmp/doc_compression_test"
	os.MkdirAll(outputRoot, 0755)
	defer os.RemoveAll(outputRoot)

	// 创建测试文档
	testDoc := &Doc{
		Key:     []byte("test_document"),
		Content: []byte(RandStringBytesMaskImprSrc(2048 * 1024)),
	}

	compressionTypes := []int{NONE, GZIP, BROTLI, BZIP2, LZ4, XZ}
	optCompressor := &OptimizedDocCompressor{}
	oldCompressor := &oldCompressor{}
	for _, compType := range compressionTypes {
		compTypeName := getCompressionTypeName(compType)
		t.Run(fmt.Sprintf("DocCompression_%s", compTypeName), func(t *testing.T) {

			// 测试原始文档压缩
			t.Run("Original", func(t *testing.T) {
				start := time.Now()

				compressedDoc, err := oldCompressor.CompressDoc(testDoc, compType)
				if err != nil {
					t.Fatalf("原始文档压缩失败: %v", err)
				}

				duration := time.Since(start)
				t.Logf("原始文档压缩 - %s: %v", compTypeName, duration)
				t.Logf("  压缩后大小: %d bytes", len(compressedDoc.Content))
			})

			// 测试优化文档压缩
			t.Run("Optimized", func(t *testing.T) {
				start := time.Now()

				compressedDoc, err := optCompressor.CompressDoc(testDoc, compType)
				if err != nil {
					t.Fatalf("优化文档压缩失败: %v", err)
				}

				duration := time.Since(start)
				t.Logf("优化文档压缩 - %s: %v", compTypeName, duration)
				t.Logf("  压缩后大小: %d bytes", len(compressedDoc.Content))
			})
		})
	}
}

// BenchmarkOptimizedCompression 基准测试优化压缩性能
func BenchmarkOptimizedCompression(b *testing.B) {
	testData := []byte(RandStringBytesMaskImprSrc(4096))
	compressionTypes := []int{GZIP, BROTLI, BZIP2, LZ4, XZ}

	for _, compType := range compressionTypes {
		compTypeName := getCompressionTypeName(compType)
		b.Run(fmt.Sprintf("Original_%s", compTypeName), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := CompressOriginal(testData, compType)
				if err != nil {
					b.Fatalf("压缩失败: %v", err)
				}
			}
		})

		b.Run(fmt.Sprintf("Optimized_%s", compTypeName), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := Compress(testData, compType)
				if err != nil {
					b.Fatalf("压缩失败: %v", err)
				}
			}
		})
	}
}

// 辅助函数：获取压缩类型名称
func getCompressionTypeName(compType int) string {
	switch compType {
	case NONE:
		return "none"
	case GZIP:
		return "gzip"
	case BROTLI:
		return "brotli"
	case BZIP2:
		return "bzip2"
	case LZ4:
		return "lz4"
	case XZ:
		return "xz"
	default:
		return "unknown"
	}
}
