package binfile

import (
	"fmt"
	"io"
	"os"
	"sync"
)

// LogLevel 日志级别
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	FATAL
)

// Logger 日志器
type Logger struct {
	level   LogLevel
	output  io.Writer
	enabled bool
	mu      sync.RWMutex
}

// GlobalLogger 全局日志器
var GlobalLogger = &Logger{
	level:   INFO,
	output:  os.Stderr,
	enabled: true,
}

// SetLevel 设置日志级别
func (l *Logger) SetLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// SetOutput 设置输出目标
func (l *Logger) SetOutput(output io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.output = output
}

// Enable 启用日志输出
func (l *Logger) Enable() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enabled = true
}

// Disable 禁用日志输出
func (l *Logger) Disable() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enabled = false
}

// IsEnabled 检查是否启用日志输出
func (l *Logger) IsEnabled() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.enabled
}

// log 内部日志方法
func (l *Logger) log(level LogLevel, format string, args ...interface{}) {
	l.mu.RLock()
	if !l.enabled || level < l.level {
		l.mu.RUnlock()
		return
	}
	output := l.output
	l.mu.RUnlock()

	fmt.Fprintf(output, format, args...)
}

// Debug 调试日志
func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(DEBUG, "[DEBUG] "+format, args...)
}

// Info 信息日志
func (l *Logger) Info(format string, args ...interface{}) {
	l.log(INFO, "[INFO] "+format, args...)
}

// Warn 警告日志
func (l *Logger) Warn(format string, args ...interface{}) {
	l.log(WARN, "[WARN] "+format, args...)
}

// Error 错误日志
func (l *Logger) Error(format string, args ...interface{}) {
	l.log(ERROR, "[ERROR] "+format, args...)
}

// Fatal 致命错误日志
func (l *Logger) Fatal(format string, args ...interface{}) {
	l.log(FATAL, "[FATAL] "+format, args...)
}

// Print 通用打印方法（兼容fmt.Printf）
func (l *Logger) Print(format string, args ...interface{}) {
	l.log(INFO, format, args...)
}

// Printf 通用打印方法（兼容fmt.Printf）
func (l *Logger) Printf(format string, args ...interface{}) {
	l.log(INFO, format+"\n", args...)
}

// Println 通用打印方法（兼容fmt.Println）
func (l *Logger) Println(args ...interface{}) {
	l.mu.RLock()
	if !l.enabled {
		l.mu.RUnlock()
		return
	}
	output := l.output
	l.mu.RUnlock()
	fmt.Fprintln(output, args...)
}

// 全局便捷函数，直接使用GlobalLogger

// SetGlobalLogLevel 设置全局日志级别
func SetGlobalLogLevel(level LogLevel) {
	GlobalLogger.SetLevel(level)
}

// EnableGlobalLog 启用全局日志
func EnableGlobalLog() {
	GlobalLogger.Enable()
}

// DisableGlobalLog 禁用全局日志
func DisableGlobalLog() {
	GlobalLogger.Disable()
}

// SetGlobalLogOutput 设置全局日志输出
func SetGlobalLogOutput(output io.Writer) {
	GlobalLogger.SetOutput(output)
}

// 全局日志函数，可以直接替换现有的fmt.Printf调用
func LogDebug(format string, args ...interface{}) {
	GlobalLogger.Debug(format, args...)
}

func LogInfo(format string, args ...interface{}) {
	GlobalLogger.Info(format, args...)
}

func LogWarn(format string, args ...interface{}) {
	GlobalLogger.Warn(format, args...)
}

func LogError(format string, args ...interface{}) {
	GlobalLogger.Error(format, args...)
}

func LogFatal(format string, args ...interface{}) {
	GlobalLogger.Fatal(format, args...)
}

// LogPrint 兼容fmt.Printf的全局函数
func LogPrint(format string, args ...interface{}) {
	GlobalLogger.Print(format, args...)
}

// LogPrintf 兼容fmt.Printf的全局函数
func LogPrintf(format string, args ...interface{}) {
	GlobalLogger.Printf(format, args...)
}

// LogPrintln 兼容fmt.Println的全局函数
func LogPrintln(args ...interface{}) {
	GlobalLogger.Println(args...)
}

// 静默模式：禁用所有日志输出
func SetQuietMode(quiet bool) {
	if quiet {
		GlobalLogger.Disable()
	} else {
		GlobalLogger.Enable()
	}
}

// 将日志重定向到/dev/null
func RedirectToDevNull() {
	devNull, err := os.OpenFile("/dev/null", os.O_WRONLY, 0)
	if err == nil {
		GlobalLogger.SetOutput(devNull)
	}
}

// 将日志重定向到文件
func RedirectToFile(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	GlobalLogger.SetOutput(file)
	return nil
}
