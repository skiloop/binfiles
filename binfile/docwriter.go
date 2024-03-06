package binfile

import (
	"bytes"
	"io"
)

type DocWriter interface {
	Write(doc *Doc) (int, error)
}

type docWriter struct {
	w io.Writer
}

func (w *docWriter) Write(doc *Doc) (int, error) {
	if nil == doc {
		return 0, nil
	}
	return doc.writeDoc(w.w)
}

func NewDocWriter(w io.Writer) DocWriter {
	return &docWriter{w: w}
}

type compressDocWriter struct {
	w          io.Writer
	buf        *bytes.Buffer
	compressor io.WriteCloser
}

func NewCompressDocWriter(writer io.Writer, compressType int) (DocWriter, error) {
	buf := &bytes.Buffer{}
	compressor, err := getCompressWriter(compressType, buf)
	if err != nil {
		return nil, err
	}
	return &compressDocWriter{w: writer, buf: buf, compressor: compressor}, nil
}

func (w *compressDocWriter) Write(doc *Doc) (nw int, err error) {
	if nil == doc {
		return 0, nil
	}
	defer w.buf.Reset()
	_, err = w.compressor.Write(doc.Content)
	_ = w.compressor.Close()
	nd := &Doc{Key: doc.Key, Content: w.buf.Bytes()}
	return nd.writeDoc(w.w)
}
