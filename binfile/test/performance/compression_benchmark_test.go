package performance

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/skiloop/binfiles/binfile"
	"github.com/skiloop/binfiles/binfile/test/common"
)

// BenchmarkCompressionOriginal 基准测试原始压缩方法
func BenchmarkCompressionOriginal(b *testing.B) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = binfile.CompressOriginal(data, binfile.GZIP)
	}
}

// BenchmarkCompressionWithPool 基准测试内存池压缩方法
func BenchmarkCompressionWithPool(b *testing.B) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = binfile.Compress(data, binfile.GZIP)
	}
}

// BenchmarkCompressionTypes 基准测试不同压缩类型
func BenchmarkCompressionTypes(b *testing.B) {
	testData := []byte(common.RandStringBytesMaskImprSrc(4096 * 1024)) // 4MB
	compressionTypes := []int{binfile.GZIP, binfile.BROTLI, binfile.BZIP2, binfile.LZ4, binfile.XZ}

	for _, compType := range compressionTypes {
		compTypeName := common.GetCompressionTypeName(compType)
		b.Run(fmt.Sprintf("Original_%s", compTypeName), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := binfile.CompressOriginal(testData, compType)
				if err != nil {
					b.Fatalf("Compression failed: %v", err)
				}
			}
		})

		b.Run(fmt.Sprintf("Optimized_%s", compTypeName), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := binfile.GlobalMemoryPool.CompressWithPool(testData, compType)
				if err != nil {
					b.Fatalf("Compression failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkDocumentCompression 基准测试文档压缩
func BenchmarkDocumentCompression(b *testing.B) {
	testDoc := &binfile.Doc{
		Key:     []byte("test_document"),
		Content: []byte(common.RandStringBytesMaskImprSrc(2048 * 1024)), // 2MB
	}

	compressionTypes := []int{binfile.NONE, binfile.GZIP, binfile.BROTLI, binfile.BZIP2, binfile.LZ4, binfile.XZ}
	optCompressor := &binfile.OptimizedDocCompressor{}
	// 使用原始压缩方法而不是oldCompressor
	// oldCompressor := &binfile.OldCompressor{}

	for _, compType := range compressionTypes {
		compTypeName := common.GetCompressionTypeName(compType)

		b.Run(fmt.Sprintf("Original_%s", compTypeName), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := binfile.CompressOriginal(testDoc.Content, compType)
				if err != nil {
					b.Fatalf("Document compression failed: %v", err)
				}
			}
		})

		b.Run(fmt.Sprintf("Optimized_%s", compTypeName), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := optCompressor.CompressDoc(testDoc, compType)
				if err != nil {
					b.Fatalf("Document compression failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkMemoryPoolReuse 基准测试内存池复用
func BenchmarkMemoryPoolReuse(b *testing.B) {
	pool := binfile.NewMemoryPool()
	testData := []byte(common.RandStringBytesMaskImprSrc(1024))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.GetBuffer()
		// 模拟使用缓冲区
		buf = append(buf, testData...)
		pool.PutBuffer(buf)
	}
}

// BenchmarkLoggerQuietMode 基准测试日志静默模式
func BenchmarkLoggerQuietMode(b *testing.B) {
	binfile.SetQuietMode(true)
	defer binfile.SetQuietMode(false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binfile.LogInfo("Benchmark message %d", i)
	}
}

// BenchmarkLoggerEnabled 基准测试日志启用模式
func BenchmarkLoggerEnabled(b *testing.B) {
	binfile.SetQuietMode(false)
	defer binfile.SetQuietMode(true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binfile.LogInfo("Benchmark message %d", i)
	}
}

// TestMemoryPoolPerformance 测试内存池性能优化效果
func TestMemoryPoolPerformance(t *testing.T) {
	// 创建测试数据
	testData := make([]byte, 1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// 测试原始压缩方法
	start := time.Now()
	for i := 0; i < 1000; i++ {
		_, err := binfile.CompressOriginal(testData, binfile.GZIP)
		if err != nil {
			t.Fatalf("CompressOriginal failed: %v", err)
		}
	}
	originalTime := time.Since(start)

	// 测试内存池压缩方法
	start = time.Now()
	for i := 0; i < 1000; i++ {
		_, err := binfile.Compress(testData, binfile.GZIP)
		if err != nil {
			t.Fatalf("Compress with pool failed: %v", err)
		}
	}
	poolTime := time.Since(start)

	t.Logf("Original compression time: %v", originalTime)
	t.Logf("Pool-based compression time: %v", poolTime)
	t.Logf("Performance improvement: %.2f%%", float64(originalTime-poolTime)/float64(originalTime)*100)

	// 内存池应该更快
	if poolTime >= originalTime {
		t.Logf("Warning: Pool-based compression is not faster than original")
	}
}

// TestCompressDocWithPool 测试文档压缩的内存池优化
func TestCompressDocWithPool(t *testing.T) {
	optCompressor := binfile.OptimizedDocCompressor{}
	// 使用原始压缩方法
	doc := &binfile.Doc{
		Key:     []byte("test-key"),
		Content: make([]byte, 512),
	}

	// 填充测试数据
	for i := range doc.Content {
		doc.Content[i] = byte(i % 256)
	}

	// 测试原始方法
	start := time.Now()
	for i := 0; i < 100; i++ {
		_, err := binfile.CompressOriginal(doc.Content, binfile.GZIP)
		if err != nil {
			t.Fatalf("CompressOriginal failed: %v", err)
		}
	}
	originalTime := time.Since(start)

	// 测试内存池方法
	start = time.Now()
	for i := 0; i < 100; i++ {
		_, err := optCompressor.CompressDoc(doc, binfile.GZIP)
		if err != nil {
			t.Fatalf("CompressDoc with pool failed: %v", err)
		}
	}
	poolTime := time.Since(start)

	t.Logf("Original doc compression time: %v", originalTime)
	t.Logf("Pool-based doc compression time: %v", poolTime)
	t.Logf("Performance improvement: %.2f%%", float64(originalTime-poolTime)/float64(originalTime)*100)
}

// TestMemoryPoolConcurrency 测试内存池的并发安全性
func TestMemoryPoolConcurrency(t *testing.T) {
	t.Run("ConcurrentCompression", func(t *testing.T) {
		const goroutines = 10
		const iterations = 100

		done := make(chan bool, goroutines)
		testData := []byte(common.RandStringBytesMaskImprSrc(1024))
		compTypes := []int{binfile.GZIP, binfile.BROTLI, binfile.BZIP2, binfile.LZ4, binfile.XZ}

		for i := 0; i < goroutines; i++ {
			go func(id int) {
				defer func() { done <- true }()

				for j := 0; j < iterations; j++ {
					// 测试不同的压缩类型
					compType := compTypes[j%len(compTypes)]

					result, err := binfile.Compress(testData, compType)
					if err != nil {
						t.Errorf("Goroutine %d, iteration %d compression failed: %v", id, j, err)
						return
					}

					if len(result) == 0 {
						t.Errorf("Goroutine %d, iteration %d compression result is empty", id, j)
						return
					}
				}
			}(i)
		}

		// 等待所有goroutine完成
		for i := 0; i < goroutines; i++ {
			<-done
		}

		t.Log("Concurrent compression test completed")
	})
}

// TestOptimizedVsOriginalPerformance 测试优化版本与原始版本的性能对比
func TestOptimizedVsOriginalPerformance(t *testing.T) {
	// 创建测试数据
	testData := []byte(common.RandStringBytesMaskImprSrc(4096))
	compressionTypes := common.GetAllCompressionTypes()
	tests := []struct {
		testType     string
		name         string
		compressFunc func([]byte, int) ([]byte, error)
	}{
		{testType: "Original", name: "Original compression", compressFunc: binfile.CompressOriginal},
		{testType: "Optimized", name: "Optimized compression", compressFunc: binfile.Compress},
	}

	for _, compType := range compressionTypes {
		compTypeName := common.GetCompressionTypeName(compType)
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
						t.Fatalf("%s compression failed: %v", test.name, err)
					}

					duration := time.Since(start)
					runtime.GC()
					runtime.ReadMemStats(&m2)

					t.Logf("%s - %s:", test.name, compTypeName)
					t.Logf("  Time: %v", duration)
					t.Logf("  Total allocations: %d", m2.Mallocs-m1.Mallocs)
					t.Logf("  Total frees: %d", m2.Frees-m1.Frees)
					t.Logf("  Current heap size: %d KB", m2.HeapAlloc/1024)
					t.Logf("  GC count: %d", m2.NumGC-m1.NumGC)
					t.Logf("  Compressed size: %d bytes", len(result))
				})
			}

		})
	}
}
