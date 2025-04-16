package binfile

import (
	"errors"
	"fmt"
	"github.com/skiloop/binfiles/binfile/filelock"
	"io"
	"os"
	"sync"
)

var errWorkersStopped = errors.New("workers stopped")

type PackageOption struct {
	Path          string `doc:"source path"`
	Pattern       string `doc:"file pattern,those match will be packaged. all files include if empty"`
	InputCompress int    `doc:"source file compression type package"`
	WorkerCount   int    `doc:"worker count"`
}

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
	docWriter    DocWriter
}

func createBinWriter(filename string, compressType int) *binWriter {

	return &binWriter{filename: filename, compressType: compressType, file: nil, mu: sync.Mutex{}}
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
	dw.docWriter = NewDocWriter(dw.file)
	return nil
}

func (dw *binWriter) lock() error {
	dw.mu.Lock()
	err := filelock.Lock(*dw.file)
	if err == nil {
		return nil
	}
	dw.mu.Unlock()
	return err
}

func (dw *binWriter) unlock() error {
	dw.mu.Lock()
	err := filelock.UnLock(*dw.file)
	if err == nil {
		dw.mu.Unlock()
		return nil
	}
	return err
}

func (dw *binWriter) Write(doc *Doc) (int, error) {
	if dw.file == nil {
		return 0, errors.New("not opened yet")
	}
	compressedDoc, err := CompressDoc(doc, dw.compressType)
	if err != nil {
		return 0, err
	}
	if err = dw.lock(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "lock file error: %v\n", err)
		return 0, err
	}
	defer func() {
		_ = dw.unlock()
	}()
	return dw.docWriter.Write(compressedDoc)
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
	if dw.packageCompressType != NONE {
		_ = dw.compressor.Close()
	}
	err := dw.file.Close()
	if err != nil {
		return err
	}
	dw.file = nil
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
	}

	dw.docWriter = NewDocWriter(pw)
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

func (dw *ccBinWriter) Write(doc *Doc) (int, error) {
	if dw.file == nil {
		return 0, errors.New("not opened yet")
	}
	compressedDoc, err := CompressDoc(doc, dw.compressType)
	if err != nil {
		return 0, err
	}
	if err = dw.lock(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "lock file error: %v\n", err)
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
	return dw.docWriter.Write(compressedDoc)
}
