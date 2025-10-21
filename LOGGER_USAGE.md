# 日志系统使用指南

这个日志系统提供了一个简单而强大的日志功能，可以方便地替换现有的 `fmt.Printf` 输出，并支持开关控制。

## 🚀 快速开始

### 基本使用

```go
// 替换现有的 fmt.Printf
LogInfo("Processing file: %s", filename)

// 替换现有的 fmt.Fprintf(os.Stderr, ...)
LogError("Error occurred: %v", err)

// 替换现有的 fmt.Println
LogPrintln("Operation completed")
```

### 开关控制

```go
// 禁用所有日志输出（静默模式）
SetQuietMode(true)

// 启用日志输出
SetQuietMode(false)

// 设置日志级别（只显示WARN及以上）
SetGlobalLogLevel(WARN)
```

## 📋 替换现有代码

### 查找和替换模式

| 原来的代码 | 替换为 |
|-----------|--------|
| `fmt.Fprintf(os.Stderr, "error: %v\n", err)` | `LogError("error: %v", err)` |
| `fmt.Printf("info: %s\n", msg)` | `LogInfo("info: %s", msg)` |
| `fmt.Println("message")` | `LogPrintln("message")` |
| `fmt.Fprint(os.Stderr, "error")` | `LogError("error")` |

### 批量替换命令

```bash
# 使用sed进行批量替换
sed -i 's/fmt\.Fprintf(os\.Stderr, /LogError(/g' *.go
sed -i 's/fmt\.Printf(/LogInfo(/g' *.go
sed -i 's/fmt\.Println(/LogPrintln(/g' *.go
```

## 🎛️ 配置选项

### 日志级别

```go
SetGlobalLogLevel(DEBUG)  // 显示所有日志
SetGlobalLogLevel(INFO)   // 显示INFO及以上（默认）
SetGlobalLogLevel(WARN)   // 只显示WARN和ERROR
SetGlobalLogLevel(ERROR)  // 只显示ERROR
```

### 输出重定向

```go
// 重定向到文件
RedirectToFile("/tmp/binfile.log")

// 重定向到/dev/null（静默输出）
RedirectToDevNull()

// 恢复默认输出到stderr
SetGlobalLogOutput(os.Stderr)
```

## 🔧 实际应用场景

### 1. 性能测试中禁用日志

```go
func BenchmarkFunction(b *testing.B) {
    SetQuietMode(true)
    defer SetQuietMode(false)
    
    for i := 0; i < b.N; i++ {
        // 测试代码...
    }
}
```

### 2. 不同环境的日志配置

```go
func main() {
    if os.Getenv("ENV") == "production" {
        SetGlobalLogLevel(WARN)
    } else {
        SetGlobalLogLevel(DEBUG)
    }
    
    if os.Getenv("QUIET") == "true" {
        SetQuietMode(true)
    }
    
    // 应用逻辑...
}
```

### 3. 条件日志输出

```go
verbose := os.Getenv("VERBOSE") == "true"
if verbose {
    LogDebug("Detailed processing information")
}
LogInfo("Operation completed") // 总是输出
```

## 📊 日志级别说明

| 级别 | 用途 | 示例 |
|------|------|------|
| DEBUG | 调试信息 | `LogDebug("Processing item %d", i)` |
| INFO | 一般信息 | `LogInfo("Operation started")` |
| WARN | 警告信息 | `LogWarn("Deprecated function used")` |
| ERROR | 错误信息 | `LogError("Failed to open file: %v", err)` |
| FATAL | 致命错误 | `LogFatal("Critical system error")` |

## 🎯 在repack功能中的应用

### 替换现有输出

```go
// 原来的代码
_, _ = fmt.Fprintf(os.Stderr, "decompress reader error: %v\n", err)

// 替换为
LogError("decompress reader error: %v", err)
```

### 在性能测试中控制输出

```go
func TestRepackPerformance(t *testing.T) {
    SetQuietMode(true)
    defer SetQuietMode(false)
    
    // 执行repack测试...
    // 所有日志输出都被禁用
}
```

## ⚡ 性能考虑

- 在静默模式下，日志函数会立即返回，几乎无性能开销
- 日志级别过滤在函数调用时进行，避免不必要的字符串格式化
- 使用 `sync.RWMutex` 确保线程安全的同时保持高性能

## 🔍 调试技巧

### 临时启用调试日志

```go
// 临时启用详细日志
SetGlobalLogLevel(DEBUG)
LogDebug("Debug information: %+v", complexStruct)
SetGlobalLogLevel(INFO) // 恢复默认级别
```

### 重定向到文件进行调试

```go
RedirectToFile("/tmp/debug.log")
LogDebug("This will be written to file")
SetGlobalLogOutput(os.Stderr) // 恢复控制台输出
```

## 📝 最佳实践

1. **使用适当的日志级别**：DEBUG用于调试，INFO用于一般信息，WARN用于警告，ERROR用于错误
2. **在性能测试中禁用日志**：使用 `SetQuietMode(true)` 避免日志输出影响性能测试结果
3. **在生产环境中提高日志级别**：只显示WARN和ERROR级别的日志
4. **使用结构化日志**：包含足够的上下文信息，如文件名、行号等
5. **避免在热路径中记录DEBUG日志**：即使禁用了输出，字符串格式化仍然有开销

## 🛠️ 扩展功能

如果需要更高级的日志功能（如结构化日志、日志轮转等），可以考虑集成第三方日志库如 `logrus` 或 `zap`，但当前的日志系统已经能够满足大多数基本需求。
