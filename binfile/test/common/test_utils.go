package common

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/skiloop/binfiles/binfile"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const (
	charSetLen = int64(len(charset))
	idxBits    = 6
	idxMask    = 1<<idxBits - 1
	idxMax     = 63 / idxBits
)

// 创建一个全局的、已播种的随机源
var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()),
)

// GetTestDir 获取测试目录
func GetTestDir(name string) string {
	// windows
	if runtime.GOOS == "windows" {
		home, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		return path.Join(home, "AppData", "Local", "Temp", fmt.Sprintf("test_%s", name))
	}
	// other os
	return path.Join("/tmp", fmt.Sprintf("test_%s", name))
}

// RandStringBytesMaskImprSrc 快速生成指定长度的随机字符串
func RandStringBytesMaskImprSrc(n int) string {
	b := make([]byte, n)
	for i, cache, remain := n-1, seededRand.Int63(), idxMax; i >= 0; {
		if remain == 0 {
			cache, remain = seededRand.Int63(), idxMax
		}

		if idx := int(cache & idxMask); idx < int(charSetLen) {
			b[i] = charset[idx]
			i--
		}

		cache >>= idxBits
		remain--
	}

	return string(b)
}

// CreateRandomDoc 创建随机文档
func CreateRandomDoc() *binfile.Doc {
	return &binfile.Doc{
		Key:     []byte(RandStringBytesMaskImprSrc(10)),
		Content: []byte(RandStringBytesMaskImprSrc(100)),
	}
}

// CreateTestDocs 创建测试文档集合
func CreateTestDocs(count int) []*binfile.Doc {
	docs := make([]*binfile.Doc, count)
	for i := 0; i < count; i++ {
		docs[i] = &binfile.Doc{
			Key:     []byte(fmt.Sprintf("test-key-%d", i)),
			Content: []byte(RandStringBytesMaskImprSrc(1024)),
		}
	}
	return docs
}

// GetCompressionTypeName 获取压缩类型名称
func GetCompressionTypeName(compType int) string {
	switch compType {
	case binfile.NONE:
		return "none"
	case binfile.GZIP:
		return "gzip"
	case binfile.BROTLI:
		return "brotli"
	case binfile.BZIP2:
		return "bzip2"
	case binfile.LZ4:
		return "lz4"
	case binfile.XZ:
		return "xz"
	default:
		return "unknown"
	}
}

// GetAllCompressionTypes 获取所有压缩类型
func GetAllCompressionTypes() []int {
	return []int{binfile.NONE, binfile.GZIP, binfile.BROTLI, binfile.BZIP2, binfile.LZ4, binfile.XZ}
}

// WriteTestFile 写入测试文件
func WriteTestFile(filename string, docs []*binfile.Doc, compressType int) error {
	bw := binfile.NewBinWriter(filename, compressType)
	err := bw.Open()
	if err != nil {
		return err
	}
	defer bw.Close()

	for _, doc := range docs {
		_, err = bw.Write(doc)
		if err != nil {
			return err
		}
	}
	return nil
}

// CleanupTestDir 清理测试目录
func CleanupTestDir(dir string) {
	os.RemoveAll(dir)
}
