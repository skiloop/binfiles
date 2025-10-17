package binfile

import (
	"fmt"
	"os"
)

// 使用示例：展示如何替换现有的fmt.Printf调用

// 示例1：替换现有的错误输出
func ExampleReplaceErrorOutput() {
	// 原来的代码：
	// _, _ = fmt.Fprintf(os.Stderr, "decompress reader error: %v\n", err)

	// 替换为：
	err := fmt.Errorf("示例错误")
	LogError("decompress reader error: %v\n", err)

	// 或者使用更简洁的方式：
	LogError("decompress reader error: %v", err)
}

// 示例2：替换现有的信息输出
func ExampleReplaceInfoOutput() {
	// 原来的代码：
	// fmt.Printf("Processing file: %s\n", filename)

	// 替换为：
	filename := "test.bin"
	LogInfo("Processing file: %s", filename)
}

// 示例3：替换现有的调试输出
func ExampleReplaceDebugOutput() {
	// 原来的代码：
	// fmt.Printf("Worker %d started\n", workerID)

	// 替换为：
	workerID := 1
	LogDebug("Worker %d started", workerID)
}

// 示例4：展示如何控制日志输出
func ExampleControlLogOutput() {
	// 禁用所有日志输出（静默模式）
	SetQuietMode(true)
	LogInfo("这条消息不会显示")

	// 启用日志输出
	SetQuietMode(false)
	LogInfo("这条消息会显示")

	// 设置日志级别（只显示WARN及以上级别）
	SetGlobalLogLevel(WARN)
	LogInfo("这条INFO消息不会显示")
	LogWarn("这条WARN消息会显示")
	LogError("这条ERROR消息会显示")

	// 重置为INFO级别
	SetGlobalLogLevel(INFO)
	LogInfo("现在INFO消息又会显示了")
}

// 示例5：重定向日志输出
func ExampleRedirectLogOutput() {
	// 重定向到/dev/null（静默输出）
	RedirectToDevNull()
	LogInfo("这条消息被重定向到/dev/null")

	// 重定向到文件
	err := RedirectToFile("/tmp/binfile.log")
	if err != nil {
		LogError("重定向到文件失败: %v", err)
		return
	}
	LogInfo("这条消息被写入到文件")

	// 恢复默认输出到stderr
	SetGlobalLogOutput(os.Stderr)
	LogInfo("这条消息输出到stderr")
}

// 示例6：在repack函数中使用
func ExampleRepackUsage() {
	// 在repack开始时
	LogInfo("Starting repack operation")

	// 处理过程中的信息
	LogDebug("Initializing %d workers", 3)

	// 错误处理
	if err := fmt.Errorf("模拟错误"); err != nil {
		LogError("Repack failed: %v", err)
		return
	}

	// 成功完成
	LogInfo("Repack completed successfully")
}

// 示例7：批量替换现有代码的模式
func ExampleBulkReplacement() {
	// 查找和替换模式：

	// 1. 查找: fmt.Fprintf(os.Stderr,
	//    替换为: LogError(

	// 2. 查找: fmt.Printf(
	//    替换为: LogInfo(

	// 3. 查找: fmt.Println(
	//    替换为: LogPrintln(

	// 4. 查找: fmt.Fprint(os.Stderr,
	//    替换为: LogError(

	// 示例替换：
	// 原来：fmt.Fprintf(os.Stderr, "decompress error: %v\n", err)
	// 替换：LogError("decompress error: %v", err)

	// 原来：fmt.Printf("Processing %d documents\n", count)
	// 替换：LogInfo("Processing %d documents", count)

	// 原来：fmt.Println("Operation completed")
	// 替换：LogPrintln("Operation completed")
}

// 示例8：条件日志输出
func ExampleConditionalLogging() {
	verbose := true

	if verbose {
		LogDebug("Verbose mode: detailed processing information")
		LogDebug("File size: %d bytes", 1024*1024)
		LogDebug("Compression ratio: %.2f", 0.75)
	}

	// 总是输出的重要信息
	LogInfo("Operation completed")

	// 错误总是输出
	LogError("Critical error occurred")
}

// 示例9：性能测试中的日志控制
func ExamplePerformanceTestLogging() {
	// 在性能测试中禁用日志输出
	SetQuietMode(true)

	// 执行性能测试...
	// 这里的所有LogInfo、LogDebug等都不会输出

	// 测试完成后恢复日志
	SetQuietMode(false)
	LogInfo("Performance test completed")
}

// 示例10：不同场景的日志配置
func ExampleScenarioConfigurations() {
	// 开发环境：显示所有日志
	SetGlobalLogLevel(DEBUG)
	EnableGlobalLog()

	// 生产环境：只显示警告和错误
	SetGlobalLogLevel(WARN)
	EnableGlobalLog()

	// 静默模式：完全禁用日志
	SetQuietMode(true)

	// 调试模式：重定向到文件
	RedirectToFile("/tmp/debug.log")
	SetGlobalLogLevel(DEBUG)
}
