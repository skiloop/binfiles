package binfile

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"

	"github.com/andybalholm/brotli"
	"github.com/dsnet/compress/bzip2"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
)

// MemoryPool 提供内存和压缩器的复用池，减少频繁的内存分配
type MemoryPool struct {
	// 字节缓冲区池，用于临时数据存储
	buffers sync.Pool
	// 压缩器缓冲区池，用于压缩操作
	compressors sync.Pool
	// 解压缩器池，按压缩类型分别存储
	decompressors map[int]*sync.Pool
	// 文档缓冲区池，用于文档数据处理
	docBuffers sync.Pool
}

// NewMemoryPool 创建新的内存池
func NewMemoryPool() *MemoryPool {
	mp := &MemoryPool{
		decompressors: make(map[int]*sync.Pool),
	}

	// 初始化缓冲区池
	mp.buffers = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 64*1024) // 64KB 初始容量
		},
	}

	// 初始化压缩器缓冲区池
	mp.compressors = sync.Pool{
		New: func() interface{} {
			return &bytes.Buffer{}
		},
	}

	// 初始化文档缓冲区池
	mp.docBuffers = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 32*1024) // 32KB 初始容量
		},
	}

	// 初始化各种解压缩器池
	mp.initDecompressorPools()

	return mp
}

// initDecompressorPools 初始化各种解压缩器池
func (mp *MemoryPool) initDecompressorPools() {
	// GZIP 解压缩器池
	mp.decompressors[GZIP] = &sync.Pool{
		New: func() interface{} {
			return &gzip.Reader{}
		},
	}

	// BROTLI 解压缩器池
	mp.decompressors[BROTLI] = &sync.Pool{
		New: func() interface{} {
			return &brotli.Reader{}
		},
	}

	// BZIP2 解压缩器池
	mp.decompressors[BZIP2] = &sync.Pool{
		New: func() interface{} {
			return &bzip2.Reader{}
		},
	}

	// LZ4 解压缩器池
	mp.decompressors[LZ4] = &sync.Pool{
		New: func() interface{} {
			return &lz4.Reader{}
		},
	}

	// XZ 解压缩器池
	mp.decompressors[XZ] = &sync.Pool{
		New: func() interface{} {
			return &xz.Reader{}
		},
	}
}

// GetBuffer 获取字节缓冲区
func (mp *MemoryPool) GetBuffer() []byte {
	buf := mp.buffers.Get().([]byte)
	return buf[:0] // 重置长度但保留容量
}

// PutBuffer 归还字节缓冲区
func (mp *MemoryPool) PutBuffer(buf []byte) {
	if cap(buf) > 1024*1024 { // 不要缓存过大的缓冲区
		return
	}
	mp.buffers.Put(buf)
}

// GetCompressorBuffer 获取压缩器缓冲区
func (mp *MemoryPool) GetCompressorBuffer() *bytes.Buffer {
	buf := mp.compressors.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// PutCompressorBuffer 归还压缩器缓冲区
func (mp *MemoryPool) PutCompressorBuffer(buf *bytes.Buffer) {
	if buf.Cap() > 1024*1024 { // 不要缓存过大的缓冲区
		return
	}
	mp.compressors.Put(buf)
}

// GetDocBuffer 获取文档缓冲区
func (mp *MemoryPool) GetDocBuffer() []byte {
	buf := mp.docBuffers.Get().([]byte)
	return buf[:0] // 重置长度但保留容量
}

// PutDocBuffer 归还文档缓冲区
func (mp *MemoryPool) PutDocBuffer(buf []byte) {
	if cap(buf) > 1024*1024 { // 不要缓存过大的缓冲区
		return
	}
	mp.docBuffers.Put(buf)
}

// GetDecompressor 获取解压缩器（如果支持池化）
func (mp *MemoryPool) GetDecompressor(compressType int) interface{} {
	if pool, exists := mp.decompressors[compressType]; exists {
		return pool.Get()
	}
	return nil
}

// PutDecompressor 归还解压缩器
func (mp *MemoryPool) PutDecompressor(compressType int, decompressor interface{}) {
	if pool, exists := mp.decompressors[compressType]; exists {
		pool.Put(decompressor)
	}
}

// CompressWithPool 使用内存池进行压缩
func (mp *MemoryPool) CompressWithPool(data []byte, compressType int) ([]byte, error) {
	buf := mp.GetCompressorBuffer()
	defer mp.PutCompressorBuffer(buf)

	w, err := getCompressor(compressType, buf)
	if err != nil {
		return nil, err
	}

	_, err = w.Write(data)
	if err != nil {
		return nil, err
	}

	err = w.Close()
	if err != nil {
		return nil, err
	}

	// 获取压缩后的数据
	dst := buf.Bytes()
	if len(dst) == 0 {
		return []byte{}, nil
	}

	// 必须复制数据，因为buf会被归还到池中
	// 这是内存池设计的权衡：复用缓冲区，但返回数据需要复制
	result := make([]byte, len(dst))
	copy(result, dst)
	return result, nil
}

// DecompressWithPool 使用内存池进行数据解压缩
func (mp *MemoryPool) DecompressWithPool(data []byte, compressType int) ([]byte, error) {
	if compressType == NONE {
		return data, nil
	}

	reader, err := getDecompressReader(compressType, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	// 使用缓冲区读取解压缩后的数据
	buf := mp.GetCompressorBuffer()
	defer mp.PutCompressorBuffer(buf)

	// 读取所有数据到缓冲区
	_, err = io.Copy(buf, reader)
	if err != nil {
		return nil, err
	}

	// 复制结果数据
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// GlobalMemoryPool 全局内存池实例
var GlobalMemoryPool = NewMemoryPool()
