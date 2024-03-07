package binfile

import (
	"bytes"
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
	Close()
	Open() error
	Filename() string
}

func NewBinWriter(filename string, compressType int) BinWriter {
	return &binWriter{filename: filename, file: nil, mu: sync.Mutex{}, compressType: compressType}
}

type binWriter struct {
	compressDocWriter
	filename     string
	file         *os.File
	mu           sync.Mutex
	compressType int
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
	dw.w = file
	if dw.buf == nil {
		dw.buf = &bytes.Buffer{}
		dw.compressor, err = getCompressWriter(dw.compressType, dw.buf)
		if err != nil {
			return err
		}
	}
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
	var err error
	if err = dw.lock(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "lock file error: %v\n", err)
		return 0, err
	}
	defer func() {
		_ = dw.unlock()
	}()
	return dw.compressDocWriter.Write(doc)
}

type ccBinWriter struct {
	binWriter
	packageCompressType int
}

func (dw *ccBinWriter) Close() {
	if dw.file != nil {
		if wc, ok := dw.w.(io.WriteCloser); ok {
			_ = wc.Close()
		}
		_ = dw.file.Close()
		dw.file = nil
	}
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
	dw.w, err = getCompressWriter(dw.packageCompressType, file)
	if err != nil {
		_ = dw.file.Close()
		return err
	}
	if dw.buf == nil {
		dw.buf = &bytes.Buffer{}
		dw.compressor, err = getCompressWriter(dw.compressType, dw.buf)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewCCBinWriter(filename string, packageCompressType, compressType int) BinWriter {
	return &ccBinWriter{
		binWriter: binWriter{
			filename:     filename,
			file:         nil,
			mu:           sync.Mutex{},
			compressType: compressType,
		},
		packageCompressType: packageCompressType,
	}
}
