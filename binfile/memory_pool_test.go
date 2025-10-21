package binfile

import (
	"testing"
	"time"
)

// TestMemoryPoolPerformance 测试内存池的性能优化效果
func TestMemoryPoolPerformance(t *testing.T) {
	// 创建测试数据
	testData := make([]byte, 1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// 测试原始压缩方法
	start := time.Now()
	for i := 0; i < 1000; i++ {
		_, err := CompressOriginal(testData, GZIP)
		if err != nil {
			t.Fatalf("CompressOriginal failed: %v", err)
		}
	}
	originalTime := time.Since(start)

	// 测试内存池压缩方法
	start = time.Now()
	for i := 0; i < 1000; i++ {
		_, err := Compress(testData, GZIP)
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

// TestMemoryPoolReuse 测试内存池的复用功能
func TestMemoryPoolReuse(t *testing.T) {
	pool := NewMemoryPool()

	// 获取和归还缓冲区
	buf1 := pool.GetBuffer()
	buf2 := pool.GetBuffer()

	// 确保获取到的是不同的缓冲区（如果缓冲区不为空）
	if len(buf1) > 0 && len(buf2) > 0 && &buf1[0] == &buf2[0] {
		t.Error("Expected different buffers")
	}

	// 归还缓冲区
	pool.PutBuffer(buf1)
	pool.PutBuffer(buf2)

	// 再次获取，应该能复用之前的缓冲区
	buf3 := pool.GetBuffer()
	buf4 := pool.GetBuffer()

	// 验证缓冲区被正确复用
	if cap(buf3) != 64*1024 || cap(buf4) != 64*1024 {
		t.Errorf("Expected buffer capacity 64KB, got %d and %d", cap(buf3), cap(buf4))
	}
}

// TestCompressDocWithPool 测试文档压缩的内存池优化
func TestCompressDocWithPool(t *testing.T) {
	optCompressor := OptimizedDocCompressor{}
	oldCompressor := oldCompressor{}
	doc := &Doc{
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
		_, err := oldCompressor.CompressDoc(doc, GZIP)
		if err != nil {
			t.Fatalf("CompressDocOriginal failed: %v", err)
		}
	}
	originalTime := time.Since(start)

	// 测试内存池方法
	start = time.Now()
	for i := 0; i < 100; i++ {
		_, err := optCompressor.CompressDoc(doc, GZIP)
		if err != nil {
			t.Fatalf("CompressDoc with pool failed: %v", err)
		}
	}
	poolTime := time.Since(start)

	t.Logf("Original doc compression time: %v", originalTime)
	t.Logf("Pool-based doc compression time: %v", poolTime)
	t.Logf("Performance improvement: %.2f%%", float64(originalTime-poolTime)/float64(originalTime)*100)
}

// BenchmarkCompressOriginal 基准测试原始压缩方法
func BenchmarkCompressOriginal(b *testing.B) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CompressOriginal(data, GZIP)
	}
}

// BenchmarkCompressWithPool 基准测试内存池压缩方法
func BenchmarkCompressWithPool(b *testing.B) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Compress(data, GZIP)
	}
}
