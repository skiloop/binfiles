package binfile

import (
	"io"
	"os"
)

type conDocWriter struct {
	w io.Writer
}

func (w *conDocWriter) Write(doc *Doc) (n int, err error) {
	n, err = w.w.Write(doc.Content)
	if err != nil {
		return n, err
	}
	c, err := w.w.Write([]byte("\n"))
	return n + c, err
}

type conDocFileWriter struct {
	filename     string
	compressType int
	file         *os.File
	conDocWriter
}

func newConDocFileWriter(filename string, compressType int) *conDocFileWriter {

	return &conDocFileWriter{
		filename:     filename,
		compressType: compressType,
		file:         nil,
		conDocWriter: conDocWriter{
			w: nil,
		},
	}
}

func (w *conDocFileWriter) Open() error {
	if w.file != nil {
		return nil
	}
	file, err := os.OpenFile(w.filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w.file = file
	if w.compressType == NONE {
		w.conDocWriter.w = file

	} else {
		w.conDocWriter.w, err = getCompressor(w.compressType, file)
		if err != nil {
			_ = file.Close()
			return err
		}
	}
	return nil
}

func (w *conDocFileWriter) Close() error {
	if nil == w.file {
		return nil
	}
	err := w.file.Close()
	if err != nil {
		return err
	}
	w.file = nil
	return nil
}
