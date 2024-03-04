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
	return &binWriter{filename: filename, file: nil, mu: sync.Mutex{}, compressType: compressType}
}

type binWriter struct {
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
	}
}

func (dw *binWriter) checkAndOpen() error {
	if dw.file != nil {
		return nil
	}
	file, err := os.OpenFile(dw.filename, writerFileFlag, 0644)
	if err != nil {
		return err
	}
	dw.file = file
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

func (dw *binWriter) Open() error {
	return dw.checkAndOpen()
}

func (dw *binWriter) Write(doc *Doc) (int, error) {
	var err error
	if err = dw.lock(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "lock file error: %v\n", err)
		return 0, err
	}
	defer func() {
		_ = dw.unlock()
	}()
	var dst *Doc
	dst, err = Compress(doc, dw.compressType)
	if err != nil {
		return 0, err
	}
	return dst.writeDoc(dw.file)
}

func (dw *binWriter) open() (err error) {
	if Debug {
		fmt.Printf("opening file %s for writing\n", dw.filename)
	}
	dw.file, err = os.OpenFile(dw.filename, writerFileFlag, 0644)
	return err
}
