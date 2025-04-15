package binfile

import (
	"errors"
	"fmt"
	"github.com/skiloop/binfiles/binfile/filelock"
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
	Close()
	Open() error
	Filename() string
}

func NewBinWriter(filename string, compressType int) BinWriter {
	return createBinWriter(filename, compressType)
}

type binWriter struct {
	filename   string
	file       *os.File
	mu         sync.Mutex
	docWriter  DocWriter
	compressor DocCompressor
}

func createBinWriter(filename string, compressType int) *binWriter {
	compressor, _ := NewDocCompressor(compressType)
	return &binWriter{filename: filename, file: nil, mu: sync.Mutex{}, compressor: compressor}
}

func (dw *binWriter) Filename() string {
	return dw.filename
}
func (dw *binWriter) Close() {
	if dw.file != nil {
		_ = dw.file.Close()
		dw.file = nil
	}
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
	data, err := dw.compressor.Compress(doc)
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
	return dw.file.Write(data)
}

type ccBinWriter struct {
	filename            string
	file                *os.File
	mu                  sync.Mutex
	docWriter           DocWriter
	compressType        int
	packageCompressType int
}

func (dw *ccBinWriter) Close() {
	//TODO implement me
	panic("implement me")
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
	compressor, err := getCompressWriter(dw.packageCompressType, file)
	if err != nil {
		dw.Close()
		return err
	}
	if dw.compressType == NONE {
		dw.docWriter = NewDocWriter(compressor)
	} else {
		dw.docWriter, err = NewCompressDocWriter(compressor, dw.compressType)
		if err != nil {
			dw.Close()
			return err
		}
	}
	return nil
}

func (dw *ccBinWriter) Filename() string {
	return dw.filename
}

func NewCCBinWriter(filename string, packageCompressType, compressType int) (BinWriter, error) {
	return &ccBinWriter{filename: filename, compressType: compressType, packageCompressType: packageCompressType}, nil
}

func (dw *ccBinWriter) Write(doc *Doc) (int, error) {
	return dw.docWriter.Write(doc)
}
