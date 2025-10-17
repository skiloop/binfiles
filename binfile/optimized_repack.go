package binfile

import (
	"io"
	"sync"
)

// OptimizedFileRepack 使用内存池优化的文件重打包器
type OptimizedFileRepack struct {
	fileRepack
	memoryPool *MemoryPool
}

// NewOptimizedFileRepack 创建优化的文件重打包器
func NewOptimizedFileRepack(original *fileRepack) *OptimizedFileRepack {
	// 避免复制包含锁的结构体
	orf := &OptimizedFileRepack{
		memoryPool: NewMemoryPool(),
	}

	// 手动复制需要的字段
	orf.docCh = original.docCh
	orf.filenameCh = original.filenameCh
	orf.stopSeeder = original.stopSeeder
	orf.reader = original.reader
	orf.limit = original.limit
	orf.target = original.target
	orf.pt = original.pt
	orf.tt = original.tt
	orf.st = original.st
	orf.split = original.split
	orf.idx.Store(original.idx.Load())

	return orf
}

// OptimizedWorker 优化的worker，使用内存池减少内存分配
func (r *OptimizedFileRepack) OptimizedWorker(no int) {
	LogInfo("optimized worker %d started\n", no)

	// 获取worker专用的缓冲区
	docBuffer := r.memoryPool.GetDocBuffer()
	defer r.memoryPool.PutDocBuffer(docBuffer)

	var err error
	rp := r.nextBinWriter()
	err = rp.Open()
	if err != nil {
		return
	}

	init := 100 * no
	count := int64(init)
	docs := 0

	optCompressor := OptimizedDocCompressor{}
	for {
		doc := <-r.docCh
		if doc == nil {
			r.docCh <- doc
			r.filenameCh <- rp.Filename()
			break
		}

		if Verbose {
			LogInfo("[%d] package %s\n", no, doc.Key)
		}

		// 使用内存池优化的压缩
		compressedDoc, err := optCompressor.CompressDoc(doc, r.tt)
		if err != nil {
			LogError("[%d] compression error: %s, %v\n", no, doc.Key, err)
			continue
		}

		_, err = rp.Write(compressedDoc)
		if err != nil {
			LogError("[%d] write error: %s, %v\n", no, doc.Key, err)
			continue
		}

		docs += 1
		count += 1

		if r.split > 0 && count%int64(r.split) == 0 {
			LogInfo("[%d] %s done with %d docs\n", no, rp.Filename(), docs)
			_ = rp.Close()
			r.filenameCh <- rp.Filename()
			rp = r.nextBinWriter()
			err = rp.Open()
			if err != nil {
				LogError("[%d] failed to get next packager: %v\n", no, err)
				break
			}
			docs = 0
		}
	}

	count -= int64(init)
	LogInfo("[%d] %s done with %d docs\n", no, rp.Filename(), docs)
	_ = rp.Close()
	LogInfo("[%d] optimized fileWorker done with %d docs\n", no, count)
}

// OptimizedDocWorker 优化的文档worker，使用内存池
func (r *docRepack) OptimizedDocWorker(no int) {
	// 获取worker专用的缓冲区
	buffer := GlobalMemoryPool.GetBuffer()
	defer GlobalMemoryPool.PutBuffer(buffer)

	end := r.pos.Add(r.step)
	offset := end - r.step
	LogInfo("[%d] optimized worker starts on %d to %d\n", no, offset, end)

	br, err := NewBinReader(r.source, r.st)
	if err != nil {
		return
	}
	reader, _ := br.(*binReader)
	count := 0
	var doc *Doc

	_ = reader.resetOffset(offset)
	optCompressor := OptimizedDocCompressor{}
	for {
		doc, err = reader.docSeeker.Read(true)
		if err != nil {
			pos, dc := reader.next(offset, end, -1, -1, nil)
			if dc == nil {
				LogInfo("[%d] no more doc after %d\n", no, offset)
				break
			}
			offset, doc = pos, dc
		} else {
			offset, err = reader.docSeeker.Seek(0, io.SeekCurrent)
			if err != nil {
				break
			}
		}
		if offset > end {
			break
		}

		// 使用内存池优化压缩
		if r.st != NONE || r.tt != NONE {
			compressedDoc, err := optCompressor.CompressDoc(doc, r.tt)
			if err != nil {
				LogError("[%d] compression error: %v\n", no, err)
				continue
			}
			doc = compressedDoc
		}

		// Safely send to the channel
		select {
		case r.docCh <- doc:
			count++
		case <-r.stopCh: // Handle stop signal
			LogInfo("[%d] optimized worker stopped\n", no)
			// tell other workers to stop
			r.stopCh <- nil
			break
		}
	}
	LogInfo("[%d] optimized worker done with %d documents\n", no, count)
}

// BatchProcessor 批量处理器，用于进一步优化I/O
type BatchProcessor struct {
	batchSize     int
	buffer        []*Doc
	mutex         sync.Mutex
	writer        BinWriter
	memoryPool    *MemoryPool
	optCompressor OptimizedDocCompressor
}

// NewBatchProcessor 创建批量处理器
func NewBatchProcessor(writer BinWriter, batchSize int) *BatchProcessor {
	return &BatchProcessor{
		batchSize:     batchSize,
		buffer:        make([]*Doc, 0, batchSize),
		writer:        writer,
		memoryPool:    NewMemoryPool(),
		optCompressor: OptimizedDocCompressor{},
	}
}

// AddDoc 添加文档到批次
func (bp *BatchProcessor) AddDoc(doc *Doc) error {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	bp.buffer = append(bp.buffer, doc)

	if len(bp.buffer) >= bp.batchSize {
		return bp.flush()
	}
	return nil
}

// Flush 刷新缓冲区
func (bp *BatchProcessor) Flush() error {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	return bp.flush()
}

// flush 内部刷新方法
func (bp *BatchProcessor) flush() error {
	if len(bp.buffer) == 0 {
		return nil
	}

	// 批量处理文档
	for _, doc := range bp.buffer {
		// 使用内存池优化压缩
		compressedDoc, err := bp.optCompressor.CompressDoc(doc, bp.writer.(*binWriter).compressType)
		if err != nil {
			return err
		}

		if _, err := bp.writer.Write(compressedDoc); err != nil {
			return err
		}
	}

	// 清空缓冲区
	bp.buffer = bp.buffer[:0]
	return nil
}

// Close 关闭批量处理器
func (bp *BatchProcessor) Close() error {
	return bp.Flush()
}
