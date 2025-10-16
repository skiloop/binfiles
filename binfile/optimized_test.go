package binfile

import (
	"fmt"
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
	total := 1
	picked = rand.Intn(total)
	for i := 0; i < total; i++ {
		content := RandStringBytesMaskImprSrc(2048)
		_doc := &Doc{Key: []byte(fmt.Sprintf("test%d", i)), Content: []byte(content)}
		if i == picked {
			doc = _doc
		}
		_, err = bw.Write(_doc)
		if err != nil {
			t.Fatalf("写入文档失败: %v", err)
		}
	}
	err = bw.Close()
	if err != nil {
		t.Fatalf("关闭写入器失败: %v", err)
	}
	return picked, doc
}

// TestOptimizedRepackFunctionality 测试优化后repack的功能
func TestOptimizedRepackFunctionality(t *testing.T) {
	outputRoot := "/tmp/optimized_repack_test"
	testFile := fmt.Sprintf("%s/test.bin", outputRoot)
	os.MkdirAll(outputRoot, 0755)
	// defer os.RemoveAll(outputRoot)
	// 初始化测试文件
	picked, doc := initTestFile(testFile, t)
	if doc == nil {
		t.Fatalf("初始化测试文件失败")
	}
	t.Run("BasicOptimizedRepack", func(t *testing.T) {
		outputFile := fmt.Sprintf("%s/basic_optimized.bin", outputRoot)

		opt := RepackCmd{
			Source:             testFile,
			Target:             outputFile,
			Workers:            3,
			Mode:               "file",
			SourceCompressType: "none",
			TargetCompressType: "gzip",
			Limit:              0,
		}

		err := Repack(opt)
		if err != nil {
			t.Fatalf("优化repack失败: %v", err)
		}

		// 检查输出文件是否存在
		if _, err := os.Stat(outputFile); os.IsNotExist(err) {
			t.Fatalf("输出文件未创建: %s", outputFile)
		}
		br, err := NewBinReader(outputFile, GZIP)
		if err != nil {
			t.Fatalf("读取输出文件失败: %v", err)
		}
		var rdoc *Doc
		pattern := fmt.Sprintf("^test%d$", picked)
		_, rdoc = br.Next(&SeekOption{Offset: 0, Pattern: pattern, KeySize: int(KeySizeLimit), DocSize: -1, End: -1})
		if rdoc == nil {
			t.Fatalf("文档不存在: %d, %s", picked, pattern)
		}
		if !reflect.DeepEqual(doc, rdoc) {
			t.Fatalf("文档不一致: %v != %v", doc, rdoc)
		}

		t.Logf("优化repack成功，输出文件: %s", outputFile)
	})
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
				defer br.Close()

				for i, expectedDoc := range testDocs {
					_, actualDoc := br.Next(&SeekOption{
						Offset:  int64(i),
						Pattern: "",
						KeySize: int(KeySizeLimit),
						DocSize: -1,
						End:     -1,
					})

					if actualDoc == nil {
						t.Fatalf("读取文档失败: index %d", i)
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

	for _, compType := range compressionTypes {
		compTypeName := getCompressionTypeName(compType)
		t.Run(fmt.Sprintf("Compression_%s", compTypeName), func(t *testing.T) {

			// 测试原始方法
			t.Run("Original", func(t *testing.T) {
				start := time.Now()
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)

				result, err := CompressOriginal(testData, compType)
				if err != nil {
					t.Fatalf("原始压缩失败: %v", err)
				}

				duration := time.Since(start)
				runtime.GC()
				runtime.ReadMemStats(&m2)

				t.Logf("原始压缩 - %s:", compTypeName)
				t.Logf("  时间: %v", duration)
				t.Logf("  内存分配: %d KB", (m2.HeapAlloc-m1.HeapAlloc)/1024)
				t.Logf("  GC次数: %d", m2.NumGC-m1.NumGC)
				t.Logf("  压缩后大小: %d bytes", len(result))
			})

			// 测试优化方法
			t.Run("Optimized", func(t *testing.T) {
				start := time.Now()
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)

				result, err := Compress(testData, compType)
				if err != nil {
					t.Fatalf("优化压缩失败: %v", err)
				}

				duration := time.Since(start)
				runtime.GC()
				runtime.ReadMemStats(&m2)

				t.Logf("优化压缩 - %s:", compTypeName)
				t.Logf("  时间: %v", duration)
				t.Logf("  内存分配: %d KB", (m2.HeapAlloc-m1.HeapAlloc)/1024)
				t.Logf("  GC次数: %d", m2.NumGC-m1.NumGC)
				t.Logf("  压缩后大小: %d bytes", len(result))
			})
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

		for i := 0; i < goroutines; i++ {
			go func(id int) {
				defer func() { done <- true }()

				for j := 0; j < iterations; j++ {
					// 测试不同的压缩类型
					compTypes := []int{GZIP, BROTLI, BZIP2}
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
		Content: []byte(RandStringBytesMaskImprSrc(2048)),
	}

	compressionTypes := []int{NONE, GZIP, BROTLI, BZIP2, LZ4, XZ}

	for _, compType := range compressionTypes {
		compTypeName := getCompressionTypeName(compType)
		t.Run(fmt.Sprintf("DocCompression_%s", compTypeName), func(t *testing.T) {

			// 测试原始文档压缩
			t.Run("Original", func(t *testing.T) {
				start := time.Now()

				oldCompressor := &oldCompressor{}
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

				optCompressor := &OptimizedDocCompressor{}
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
