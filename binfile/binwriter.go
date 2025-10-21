package binfile

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/skiloop/binfiles/binfile/filelock"
)

var errWorkersStopped = errors.New("workers stopped")

type BinWriter interface {
	DocWriter
	// Close writer
	Close() error
	Open() error
	Filename() string
}

func NewBinWriter(filename string, compressType int) BinWriter {
	return createBinWriter(filename, compressType)
}

type binWriter struct {
	filename     string
	compressType int
	file         *os.File
	mu           sync.Mutex
	writer       io.Writer
}

func createBinWriter(filename string, compressType int) *binWriter {

	return &binWriter{
		filename:     filename,
		compressType: compressType,
		file:         nil,
		mu:           sync.Mutex{},
		writer:       nil,
	}
}

func (dw *binWriter) Filename() string {
	return dw.filename
}
func (dw *binWriter) Close() error {
	if dw.file != nil {
		_ = dw.file.Close()
		dw.file = nil
	}
	return nil
}

func (dw *binWriter) Open() error {
	if dw.file != nil {
		return nil
	}
	file, err := os.OpenFile(dw.filename, writerFileFlag, 0644)
	if err != nil {
		return err
	}
	dw.file = file
	dw.writer = dw.file
	return nil
}

func (dw *binWriter) lock() error {
	dw.mu.Lock()
	err := filelock.Lock(*dw.file)
	if err != nil {
		dw.mu.Unlock()
		return err
	}
	return nil
}

func (dw *binWriter) unlock() error {
	err := filelock.UnLock(*dw.file)
	if err != nil {
		return err
	}
	dw.mu.Unlock()
	return nil
}

type oldBinWriter struct {
	*binWriter
	oldCompressor oldCompressor
}

func (dw *oldBinWriter) Write(doc *Doc) (int, error) {
	if dw.file == nil {
		return 0, errors.New("not opened yet")
	}
	compressedDoc, err := dw.oldCompressor.CompressDoc(doc, dw.compressType)
	if err != nil {
		return 0, err
	}
	// LogInfo("write doc: %s, %d -> %d\n", string(compressedDoc.Key), len(doc.Content), len(compressedDoc.Content))
	if err = dw.lock(); err != nil {
		LogError("lock file error: %v\n", err)
		return 0, err
	}
	defer func() {
		_ = dw.unlock()
	}()
	return compressedDoc.writeDoc(dw.writer)
}

func (dw *binWriter) Write(doc *Doc) (int, error) {
	if dw.file == nil {
		return 0, errors.New("not opened yet")
	}
	compressedDoc, err := CompressDoc(doc, dw.compressType)
	if err != nil {
		return 0, err
	}
	// LogInfo("write doc: %s, %d -> %d\n", string(compressedDoc.Key), len(doc.Content), len(compressedDoc.Content))
	if err = dw.lock(); err != nil {
		LogError("lock file error: %v\n", err)
		return 0, err
	}
	defer func() {
		_ = dw.unlock()
	}()
	return compressedDoc.writeDoc(dw.writer)
}

type ccBinWriter struct {
	binWriter
	packageCompressType int
	compressor          Compressor
}

func (dw *ccBinWriter) Close() error {
	if dw.file == nil {
		return nil
	}
	if dw.packageCompressType != NONE && dw.compressor != nil {
		_ = dw.compressor.Close()
	}
	err := dw.file.Close()
	if err != nil {
		return err
	}
	dw.file = nil
	dw.writer = nil
	return nil
}

func (dw *ccBinWriter) Open() error {
	if dw.file != nil {
		return nil
	}
	file, err := os.OpenFile(dw.filename, writerFileFlag, 0644)
	if err != nil {
		return err
	}
	dw.compressor = nil
	dw.file = file
	var pw io.Writer
	if dw.packageCompressType == NONE {
		pw = dw.file
	} else {
		pw, err = getCompressor(dw.packageCompressType, file)
		if err != nil {
			_ = file.Close()
			return err
		}
		LogInfo("open package writer: %d, %v\n", dw.packageCompressType, pw)
		dw.compressor = pw.(Compressor)
	}

	dw.writer = pw
	return nil
}

func NewCCBinWriter(filename string, packageCompressType, compressType int) (BinWriter, error) {
	bw := &ccBinWriter{
		binWriter: binWriter{
			filename:     filename,
			compressType: compressType,
			file:         nil,
			mu:           sync.Mutex{},
		},
		packageCompressType: packageCompressType,
	}
	return bw, nil
}

func NewOldCCBinWriter(filename string, packageCompressType, compressType int) (BinWriter, error) {
	bw := &oldCCBinWriter{
		ccBinWriter: ccBinWriter{
			binWriter: binWriter{
				filename:     filename,
				compressType: compressType,
				file:         nil,
				mu:           sync.Mutex{},
			},
			packageCompressType: packageCompressType,
		},
		oldCompressor: oldCompressor{},
	}
	return bw, nil
}

type oldCCBinWriter struct {
	ccBinWriter
	oldCompressor oldCompressor
}

func (dw *oldCCBinWriter) Write(doc *Doc) (int, error) {
	if dw.file == nil {
		return 0, errors.New("not opened yet")
	}
	compressedDoc, err := dw.oldCompressor.CompressDoc(doc, dw.compressType)
	if err != nil {
		return 0, err
	}
	if err = dw.lock(); err != nil {
		LogError("lock file error: %v\n", err)
		return 0, err
	}
	defer func() {
		_ = dw.unlock()
	}()
	return compressedDoc.writeDoc(dw.writer)
}

func (dw *ccBinWriter) Write(doc *Doc) (int, error) {
	if dw.file == nil {
		return 0, errors.New("not opened yet")
	}
	compressedDoc, err := CompressDoc(doc, dw.compressType)
	if err != nil {
		return 0, err
	}
	if err = dw.lock(); err != nil {
		LogError("lock file error: %v\n", err)
		return 0, err
	}

	defer func() {
		_ = dw.unlock()
	}()
	if dw.packageCompressType != NONE {
		defer func() {
			_ = dw.compressor.Flush()
		}()
	}
	return compressedDoc.writeDoc(dw.writer)
}
