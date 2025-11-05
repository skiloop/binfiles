package binfile

import (
	"bytes"
	"testing"
)

// TestCompressionDecompression 测试压缩和解压缩功能
func TestCompressionDecompression(t *testing.T) {
	compressTypes := GetAllCompressionTypes()
	data := []byte(RandStringBytesMaskImprSrc(1024))

	for _, compressType := range compressTypes {
		t.Run(GetCompressionTypeName(compressType), func(t *testing.T) {
			// 测试原始压缩方法
			compressed, err := CompressOriginal(data, compressType)
			if err != nil {
				t.Fatalf("compress error: %v", err)
			}

			decompressed, err := Decompress(compressed, compressType)
			if err != nil {
				t.Fatalf("decompress error: %v", err)
			}

			if !bytes.Equal(data, decompressed) {
				t.Fatalf("decompress error: data mismatch")
			}
		})
	}
}

// TestMemoryPoolCompression 测试内存池压缩功能
func TestMemoryPoolCompression(t *testing.T) {
	compressTypes := GetAllCompressionTypes()
	data := []byte(RandStringBytesMaskImprSrc(1024))

	for _, compressType := range compressTypes {
		t.Run(GetCompressionTypeName(compressType), func(t *testing.T) {
			compressed, err := GlobalMemoryPool.CompressWithPool(data, compressType)
			if err != nil || len(compressed) == 0 {
				t.Fatalf("compress error: %v", err)
			}

			decompressed, err := DecompressOriginal(compressed, compressType)
			if err != nil {
				t.Fatalf("decompress error: %v", err)
			}

			if !bytes.Equal(data, decompressed) {
				t.Fatalf("decompress error: data mismatch")
			}
		})
	}
}

// TestDocCompression 测试文档压缩功能
func TestDocCompression(t *testing.T) {
	compressTypes := GetAllCompressionTypes()
	doc := &Doc{
		Key:     []byte("test-key"),
		Content: []byte(RandStringBytesMaskImprSrc(1024)),
	}

	for _, compressType := range compressTypes {
		t.Run(GetCompressionTypeName(compressType), func(t *testing.T) {
			compressed, err := CompressDoc(doc, compressType)
			if err != nil {
				t.Fatalf("compress error: %v", err)
			}

			decompressed, err := DecompressDoc(compressed, compressType, false)
			if err != nil {
				t.Fatalf("decompress error: %v", err)
			}

			if !bytes.Equal(doc.Key, decompressed.Key) || !bytes.Equal(doc.Content, decompressed.Content) {
				t.Fatalf("decompress error: data mismatch")
			}
		})
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

// TestLoggerBasic 测试日志系统基本功能
func TestLoggerBasic(t *testing.T) {
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
		"Info message",
		"[WARN] Warn message",
		"[ERROR] Error message",
	}

	for _, expected := range expectedMessages {
		if !bytes.Contains(buf.Bytes(), []byte(expected)) {
			t.Errorf("Expected output to contain %q, got %q", expected, buf.String())
		}
	}
}

// TestLoggerQuietMode 测试日志静默模式
func TestLoggerQuietMode(t *testing.T) {
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

// TestLoggerLevels 测试日志级别过滤
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
