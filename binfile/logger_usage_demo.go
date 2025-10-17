package binfile

import (
	"fmt"
	"os"
)

// 这个文件展示如何在现有代码中替换输出函数

// 示例1：替换decompressor.go中的错误输出
func ExampleReplaceDecompressorOutput() {
	// 原来的代码：
	// _, _ = fmt.Fprintf(os.Stderr, "decompress reader error: %v\n", err)

	// 替换为：
	err := fmt.Errorf("示例错误")
	LogError("decompress reader error: %v", err)
}

// 示例2：替换package.go中的错误输出
func ExampleReplacePackageOutput() {
	// 原来的代码：
	// _, _ = fmt.Fprintf(os.Stderr, "decompress error: %v\n", err)

	// 替换为：
	err := fmt.Errorf("示例错误")
	LogError("decompress error: %v", err)
}

// 示例3：替换doc.go中的错误输出
func ExampleReplaceDocOutput() {
	// 原来的代码：
	// _, _ = fmt.Fprintf(os.Stderr, "read doc content error: %v\n", err)

	// 替换为：
	err := fmt.Errorf("示例错误")
	LogError("read doc content error: %v", err)
}

// 示例4：在repack过程中使用日志
func ExampleRepackLogging() {
	// 开始repack
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

// 示例5：性能测试中的日志控制
func ExamplePerformanceTestLoggingDemo() {
	// 在性能测试开始时禁用日志
	SetQuietMode(true)
	defer SetQuietMode(false) // 测试结束后恢复

	// 执行性能测试...
	LogInfo("这条消息不会显示")
	LogError("这条错误消息也不会显示")

	// 测试完成后恢复日志
	LogInfo("Performance test completed")
}

// 示例6：不同环境的日志配置
func ExampleEnvironmentConfigurationsDemo() {
	// 开发环境
	if os.Getenv("ENV") == "development" {
		SetGlobalLogLevel(DEBUG)
		EnableGlobalLog()
		LogDebug("Development mode: showing all logs")
	}

	// 生产环境
	if os.Getenv("ENV") == "production" {
		SetGlobalLogLevel(WARN)
		EnableGlobalLog()
		LogInfo("Production mode: only warnings and errors")
	}

	// 测试环境
	if os.Getenv("ENV") == "test" {
		SetQuietMode(true)
		LogInfo("Test mode: logs disabled")
	}
}

// 示例7：条件日志输出
func ExampleConditionalLoggingDemo() {
	verbose := os.Getenv("VERBOSE") == "true"

	if verbose {
		LogDebug("Verbose mode enabled")
		LogDebug("Processing file: %s", "example.bin")
		LogDebug("File size: %d bytes", 1024*1024)
	}

	// 重要的信息总是输出
	LogInfo("Operation started")

	// 错误总是输出
	LogError("Critical error occurred")
}

// 示例8：日志重定向
func ExampleLogRedirection() {
	// 重定向到文件
	err := RedirectToFile("/tmp/binfile.log")
	if err != nil {
		LogError("Failed to redirect logs to file: %v", err)
		return
	}

	LogInfo("This message will be written to /tmp/binfile.log")

	// 恢复默认输出
	SetGlobalLogOutput(os.Stderr)
	LogInfo("This message will go to stderr")
}

// 示例9：批量替换现有代码的脚本示例
func ExampleBulkReplacementScript() {
	// 可以使用以下sed命令进行批量替换：

	// 1. 替换 fmt.Fprintf(os.Stderr,
	// sed -i 's/fmt\.Fprintf(os\.Stderr, /LogError(/g' *.go

	// 2. 替换 fmt.Printf(
	// sed -i 's/fmt\.Printf(/LogInfo(/g' *.go

	// 3. 替换 fmt.Println(
	// sed -i 's/fmt\.Println(/LogPrintln(/g' *.go

	// 4. 替换 fmt.Fprint(os.Stderr,
	// sed -i 's/fmt\.Fprint(os\.Stderr, /LogError(/g' *.go

	LogInfo("Bulk replacement examples shown above")
}

// 示例10：在main函数中配置日志
func ExampleMainFunctionConfiguration() {
	// 在main函数开始处配置日志
	// 根据命令行参数或环境变量配置日志
	if len(os.Args) > 1 && os.Args[1] == "--quiet" {
		SetQuietMode(true)
	} else if len(os.Args) > 1 && os.Args[1] == "--verbose" {
		SetGlobalLogLevel(DEBUG)
	}

	// 如果设置了日志文件环境变量，重定向到文件
	if logFile := os.Getenv("LOGFILE"); logFile != "" {
		err := RedirectToFile(logFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to redirect logs: %v\n", err)
		}
	}

	LogInfo("Application started")

	// 执行主要逻辑...
}
