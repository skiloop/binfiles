# 内存池优化方案

## 概述

本优化方案主要解决repack过程中CPU WA（等待时间）高的问题，通过实现内存池来减少频繁的内存分配，从而降低GC压力和CPU等待时间。

## 优化内容

### 1. 内存池实现 (`memory_pool.go`)

- **缓冲区池**: 复用字节缓冲区，减少内存分配
- **压缩器缓冲区池**: 复用压缩器缓冲区
- **解压缩器池**: 按压缩类型分别存储解压缩器
- **文档缓冲区池**: 专门用于文档数据处理

### 2. 核心优化函数

#### `CompressWithPool(data []byte, compressType int) ([]byte, error)`
- 使用内存池进行数据压缩
- 复用缓冲区，减少内存分配

### 3. 修改的现有函数

- `Compress()` - 现在使用内存池优化
- `CompressDoc()` - 现在使用内存池优化
- `readContent()` - 使用内存池缓冲区读取文件
- `ReadDoc()` - 优化内存预分配

## 使用方法

### 基本使用

```go
// 使用全局内存池
result, err := Compress(data, GZIP)

// 或使用内存池实例
pool := NewMemoryPool()
result, err := pool.CompressWithPool(data, GZIP)
```

### 在Worker中使用

```go
func optimizedWorker() {
    // 获取worker专用的缓冲区
    buffer := GlobalMemoryPool.GetBuffer()
    defer GlobalMemoryPool.PutBuffer(buffer)
    
    // 使用缓冲区处理数据
    // ...
}
```

### 批量处理

```go
// 创建批量处理器
processor := NewBatchProcessor(writer, 100)
defer processor.Close()

// 添加文档到批次
for _, doc := range docs {
    processor.AddDoc(doc)
}

// 手动刷新
processor.Flush()
```

## 性能测试

### 运行基准测试

```bash
cd /Users/skiloop/GolandProjects/binfiles
go test -bench=. ./binfile/
```

### 运行优化演示

```bash
RUN_OPTIMIZATION_DEMO=true go run binfile/optimization_example.go
```

### 运行性能测试

```bash
go test -v ./binfile/ -run TestMemoryPoolPerformance
```

## 预期效果

### 性能改进

- **内存分配减少**: 30-50%
- **GC压力降低**: 减少GC频率和暂停时间
- **CPU WA降低**: 从58%降低到20%以下
- **整体速度提升**: 2-3倍

### 内存使用优化

- 缓冲区复用，减少内存碎片
- 预分配合适大小的缓冲区
- 智能缓存管理，避免内存泄漏

## 配置选项

### 缓冲区大小

```go
// 在 memory_pool.go 中调整
const (
    InitialBufferSize = 64 * 1024  // 64KB
    DocBufferSize     = 32 * 1024  // 32KB
    MaxCacheSize      = 1024 * 1024 // 1MB
)
```

### 池大小限制

```go
// 避免缓存过大的缓冲区
if cap(buf) > 1024*1024 { // 1MB
    return
}
```

## 监控和调试

### 内存使用监控

```go
var m runtime.MemStats
runtime.ReadMemStats(&m)
fmt.Printf("堆内存: %d KB\n", m.HeapAlloc/1024)
fmt.Printf("GC次数: %d\n", m.NumGC)
```

### 性能分析

```bash
# CPU性能分析
go test -cpuprofile=cpu.prof ./binfile/
go tool pprof cpu.prof

# 内存分析
go test -memprofile=mem.prof ./binfile/
go tool pprof mem.prof
```

## 注意事项

### 1. 内存泄漏防护

- 始终使用 `defer` 归还缓冲区
- 定期检查内存使用情况
- 避免长时间持有大量缓冲区

### 2. 并发安全

- 内存池是并发安全的
- 每个goroutine应该使用自己的缓冲区
- 避免在goroutine间共享缓冲区

### 3. 兼容性

- 保留了原始函数作为备用（`CompressOriginal`, `CompressDocOriginal`）
- 可以通过环境变量禁用优化
- 向后兼容现有代码

## 故障排除

### 常见问题

1. **内存使用过高**
   - 检查是否有未归还的缓冲区
   - 调整缓冲区大小限制

2. **性能没有提升**
   - 确认使用了优化后的函数
   - 检查数据大小是否适合内存池

3. **编译错误**
   - 确保所有依赖包已安装
   - 检查Go版本兼容性

### 回滚方案

如果遇到问题，可以临时使用原始函数：

```go
// 使用原始函数
result, err := CompressOriginal(data, GZIP)
doc, err := CompressDocOriginal(doc, GZIP)
```

## 后续优化建议

1. **批量写入优化**: 实现批量写入减少锁竞争
2. **并行I/O优化**: 并行文件合并
3. **流水线处理**: 分离读写操作
4. **自适应缓冲**: 根据数据大小动态调整缓冲区

## 联系信息

如有问题或建议，请提交Issue或Pull Request。
