package binfile

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"time"
)

// OptimizationExample 展示如何使用内存池优化
func OptimizationExample() {
	LogInfo("=== 内存池优化示例 ===")

	// 显示当前内存使用情况
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// 创建测试数据
	testDocs := make([]*Doc, 1000)
	for i := range testDocs {
		content := make([]byte, 1024)
		for j := range content {
			content[j] = byte((i + j) % 256)
		}
		testDocs[i] = &Doc{
			Key:     []byte(fmt.Sprintf("test-key-%d", i)),
			Content: content,
		}
	}

	LogInfo("创建了 %d 个测试文档\n", len(testDocs))

	// 测试原始压缩方法
	LogInfo("\n--- 测试原始压缩方法 ---")
	oldCompressor := oldCompressor{}

	start := time.Now()
	for i, doc := range testDocs {
		_, err := oldCompressor.CompressDoc(doc, GZIP)
		if err != nil {
			LogInfo("原始压缩失败 %d: %v\n", i, err)
		}
	}
	originalTime := time.Since(start)
	runtime.ReadMemStats(&m2)
	originalMem := m2.Alloc - m1.Alloc

	// 测试内存池压缩方法
	LogInfo("\n--- 测试内存池压缩方法 ---")
	optCompressor := OptimizedDocCompressor{}

	start = time.Now()

	for i, doc := range testDocs {
		_, err := optCompressor.CompressDoc(doc, GZIP)
		if err != nil {
			LogInfo("内存池压缩失败 %d: %v\n", i, err)
		}
	}
	poolTime := time.Since(start)
	runtime.ReadMemStats(&m1)
	poolMem := m1.Alloc - m2.Alloc

	// 显示结果
	LogInfo("\n=== 性能对比结果 ===")
	LogInfo("原始方法:\n")
	LogInfo("  时间: %v\n", originalTime)
	LogInfo("  内存分配: %d bytes\n", originalMem)
	LogInfo("内存池方法:\n")
	LogInfo("  时间: %v\n", poolTime)
	LogInfo("  内存分配: %d bytes\n", poolMem)

	if originalTime > 0 {
		timeImprovement := float64(originalTime-poolTime) / float64(originalTime) * 100
		LogInfo("时间改进: %.2f%%\n", timeImprovement)
	}

	if originalMem > 0 {
		memImprovement := float64(originalMem-poolMem) / float64(originalMem) * 100
		LogInfo("内存改进: %.2f%%\n", memImprovement)
	}

	// 显示GC统计
	runtime.GC()
	var m3 runtime.MemStats
	runtime.ReadMemStats(&m3)
	LogInfo("\nGC统计:\n")
	LogInfo("  总GC次数: %d\n", m3.NumGC)
	LogInfo("  总GC时间: %v\n", time.Duration(m3.PauseTotalNs))
}

// BenchmarkMemoryAllocation 内存分配基准测试
func BenchmarkMemoryAllocation() {
	LogInfo("\n=== 内存分配基准测试 ===")

	// 创建内存池
	pool := NewMemoryPool()

	// 测试缓冲区复用
	LogInfo("测试缓冲区复用...")
	buffers := make([][]byte, 100)

	// 获取缓冲区
	for i := range buffers {
		buffers[i] = pool.GetBuffer()
	}

	// 归还缓冲区
	for _, buf := range buffers {
		pool.PutBuffer(buf)
	}

	// 再次获取，验证复用
	reusedBuffers := make([][]byte, 100)
	for i := range reusedBuffers {
		reusedBuffers[i] = pool.GetBuffer()
	}

	LogInfo("获取了 %d 个缓冲区\n", len(reusedBuffers))
	LogInfo("缓冲区容量: %d bytes\n", cap(reusedBuffers[0]))

	// 测试压缩器缓冲区复用
	LogInfo("\n测试压缩器缓冲区复用...")
	compressors := make([]*bytes.Buffer, 50)

	// 获取压缩器缓冲区
	for i := range compressors {
		compressors[i] = pool.GetCompressorBuffer()
	}

	// 归还压缩器缓冲区
	for _, buf := range compressors {
		pool.PutCompressorBuffer(buf)
	}

	LogInfo("获取了 %d 个压缩器缓冲区\n", len(compressors))
	LogInfo("压缩器缓冲区容量: %d bytes\n", compressors[0].Cap())
}

// RunOptimizationDemo 运行优化演示
func RunOptimizationDemo() {
	LogInfo("开始内存池优化演示...")

	// 设置一些环境变量用于演示
	originalVerbose := Verbose
	Verbose = true
	defer func() { Verbose = originalVerbose }()

	// 运行示例
	OptimizationExample()
	BenchmarkMemoryAllocation()

	LogInfo("\n=== 优化建议 ===")
	LogInfo("1. 使用 GlobalMemoryPool 进行内存复用")
	LogInfo("2. 在 worker 中使用 GetBuffer() 和 PutBuffer()")
	LogInfo("3. 使用 CompressDocWithPool() 进行文档压缩")
	LogInfo("4. 定期调用 runtime.GC() 清理内存")
	LogInfo("5. 监控内存使用情况，调整缓冲区大小")

	// 显示当前内存状态
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	LogInfo("\n当前内存状态:\n")
	LogInfo("  堆内存: %d KB\n", m.HeapAlloc/1024)
	LogInfo("  系统内存: %d KB\n", m.Sys/1024)
	LogInfo("  GC次数: %d\n", m.NumGC)
}

// 如果直接运行此文件，执行演示
func init() {
	// 可以通过环境变量控制是否运行演示
	if os.Getenv("RUN_OPTIMIZATION_DEMO") == "true" {
		RunOptimizationDemo()
	}
}
