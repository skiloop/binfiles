package binfile

import (
	"io"
	"os"
)

type binWriterFile struct {
	binFile
	file io.WriteCloser
	fn   *os.File
}

func newBinWriterFile(filename string, compressType int) *binWriterFile {
	return &binWriterFile{binFile: binFile{filename: filename, compressType: compressType}}
}

func (dr *binWriterFile) Close() {
	if dr.file != nil {
		_ = dr.file.Close()
		dr.file = nil
	}
}

func (dr *binWriterFile) Open() error {
	if dr.file != nil {
		return nil
	}
	fn, err := os.OpenFile(dr.filename, writerFileFlag, 0644)
	if err != nil {
		return err
	}
	dr.fn = fn
	dr.file = fn
	return err
}
