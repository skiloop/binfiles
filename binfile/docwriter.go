package binfile

import (
	"errors"
	"fmt"
	"github.com/skiloop/binfiles/binfile/filelock"
	"os"
	"sync"
)

type docWriter struct {
	binWriterFile
	fn *os.File
	mu sync.Mutex
}

var workersStopped = errors.New("workers stopped")

func (dw *docWriter) checkAndOpen() error {
	if dw.file != nil {
		return nil
	}
	fn, err := os.OpenFile(dw.filename, writerFileFlag, 0644)
	if err != nil {
		return err
	}
	dw.fn = fn
	dw.file = fn
	return nil
}

func (dw *docWriter) lock() error {
	dw.mu.Lock()
	err := filelock.Lock(*dw.fn)
	if err == nil {
		return nil
	}
	dw.mu.Unlock()
	return err
}

func (dw *docWriter) unlock() error {
	err := filelock.UnLock(*dw.fn)
	if err == nil {
		dw.mu.Unlock()
		return nil
	}
	return err
}

func (dw *docWriter) Open() error {
	return dw.checkAndOpen()
}

func (dw *docWriter) Write(doc *Doc) error {
	var err error
	if err = dw.lock(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "lock file error: %v\n", err)
		return err
	}
	defer func() {
		_ = dw.unlock()
	}()
	doc.CompressType = dw.compressType
	if err = doc.writeDoc(dw.file); Verbose && err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return err
	}
	return nil
}

func (dw *docWriter) open() (err error) {
	if Debug {
		fmt.Printf("opening file %s for writing\n", dw.filename)
	}
	dw.file, err = os.OpenFile(dw.filename, writerFileFlag, 0644)
	return err
}
