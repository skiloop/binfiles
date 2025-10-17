package binfile

import (
	"bytes"
	"os"
	"testing"
)

func TestLoggerBasic(t *testing.T) {
	// 创建测试用的缓冲区
	var buf bytes.Buffer

	// 设置日志输出到缓冲区
	SetGlobalLogOutput(&buf)
	EnableGlobalLog()
	SetGlobalLogLevel(DEBUG)

	// 测试各种日志级别
	LogDebug("Debug message")
	LogInfo("Info message")
	LogWarn("Warn message")
	LogError("Error message")

	// 检查输出
	if buf.String() == "" {
		t.Error("Expected log output, got empty string")
	}

	// 验证包含预期的消息
	expectedMessages := []string{
		"[DEBUG] Debug message",
		"[INFO] Info message",
		"[WARN] Warn message",
		"[ERROR] Error message",
	}

	for _, expected := range expectedMessages {
		if !bytes.Contains(buf.Bytes(), []byte(expected)) {
			t.Errorf("Expected output to contain %q, got %q", expected, buf.String())
		}
	}
}

func TestLoggerQuietMode(t *testing.T) {
	// 创建测试用的缓冲区
	var buf bytes.Buffer
	SetGlobalLogOutput(&buf)

	// 测试静默模式
	SetQuietMode(true)
	LogInfo("This should not appear")

	if buf.Len() > 0 {
		t.Error("Expected no output in quiet mode, but got:", buf.String())
	}

	// 恢复输出
	SetQuietMode(false)
	LogInfo("This should appear")

	if buf.Len() == 0 {
		t.Error("Expected output after disabling quiet mode")
	}
}

func TestLoggerLevels(t *testing.T) {
	var buf bytes.Buffer
	SetGlobalLogOutput(&buf)
	EnableGlobalLog()

	// 测试不同日志级别
	SetGlobalLogLevel(WARN)

	LogDebug("Debug - should not appear")
	LogInfo("Info - should not appear")
	LogWarn("Warn - should appear")
	LogError("Error - should appear")

	// 检查DEBUG和INFO消息不应该出现
	if bytes.Contains(buf.Bytes(), []byte("Debug - should not appear")) {
		t.Error("DEBUG message should not appear when level is WARN")
	}
	if bytes.Contains(buf.Bytes(), []byte("Info - should not appear")) {
		t.Error("INFO message should not appear when level is WARN")
	}

	// 检查WARN和ERROR消息应该出现
	if !bytes.Contains(buf.Bytes(), []byte("Warn - should appear")) {
		t.Error("WARN message should appear when level is WARN")
	}
	if !bytes.Contains(buf.Bytes(), []byte("Error - should appear")) {
		t.Error("ERROR message should appear when level is WARN")
	}
}

func TestLoggerCompatibility(t *testing.T) {
	var buf bytes.Buffer
	SetGlobalLogOutput(&buf)
	EnableGlobalLog()
	SetGlobalLogLevel(INFO) // 确保级别正确

	// 测试兼容性函数
	LogPrintf("Printf message: %s", "test")
	LogPrintln("Println message")

	if !bytes.Contains(buf.Bytes(), []byte("Printf message: test")) {
		t.Errorf("LogPrintf should work like fmt.Printf, got: %q", buf.String())
	}
	if !bytes.Contains(buf.Bytes(), []byte("Println message")) {
		t.Errorf("LogPrintln should work like fmt.Println, got: %q", buf.String())
	}
}

func TestLoggerRedirectToDevNull(t *testing.T) {
	// 保存原始状态
	originalOutput := GlobalLogger.output
	originalEnabled := GlobalLogger.enabled

	// 重定向到/dev/null
	RedirectToDevNull()
	EnableGlobalLog()

	// 这应该不会产生错误
	LogInfo("Message to dev/null")
	LogError("Error to dev/null")

	// 恢复原始状态
	SetGlobalLogOutput(originalOutput)
	if originalEnabled {
		EnableGlobalLog()
	} else {
		DisableGlobalLog()
	}
}

func TestLoggerRedirectToFile(t *testing.T) {
	// 测试重定向到文件
	tempFile := "/tmp/test_logger.log"

	err := RedirectToFile(tempFile)
	if err != nil {
		t.Fatalf("Failed to redirect to file: %v", err)
	}
	defer os.Remove(tempFile)

	EnableGlobalLog()
	LogInfo("Test message to file")

	// 检查文件是否存在
	if _, err := os.Stat(tempFile); os.IsNotExist(err) {
		t.Error("Log file should exist")
	}

	// 恢复默认输出
	SetGlobalLogOutput(os.Stderr)
}

func BenchmarkLogger(b *testing.B) {
	// 禁用日志输出进行性能测试
	SetQuietMode(true)
	defer SetQuietMode(false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LogInfo("Benchmark message %d", i)
	}
}

func BenchmarkLoggerEnabled(b *testing.B) {
	var buf bytes.Buffer
	SetGlobalLogOutput(&buf)
	EnableGlobalLog()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LogInfo("Benchmark message %d", i)
	}
}
