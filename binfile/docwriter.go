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

type compressDocWriter struct {
	w          io.Writer
	compressor DocCompressor
}

func (w *compressDocWriter) Write(doc *Doc) (int, error) {
	if nil == doc {
		return 0, nil
	}
	_, err := w.compressor.Compress(doc)
	if err != nil {
		return 0, err
	}
	return doc.writeDoc(w.w)
}
func NewCompressDocWriter(w io.Writer, compressType int) (DocWriter, error) {
	compressor, err := NewDocCompressor(compressType)
	if err != nil {
		return nil, err
	}
	return &compressDocWriter{w: w, compressor: compressor}, nil
}

type DocCompressor interface {
	Compress(doc *Doc) ([]byte, error)
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

type docCompressor struct {
	buf        *bytes.Buffer
	comBuf     *bytes.Buffer
	compressor io.WriteCloser
}

func NewDocCompressor(compressType int) (DocCompressor, error) {
	comBuf := &bytes.Buffer{}
	compressor, err := getCompressWriter(compressType, comBuf)
	if err != nil {
		return nil, err
	}
	return &docCompressor{buf: &bytes.Buffer{}, comBuf: comBuf, compressor: compressor}, nil
}

func (w *docCompressor) Compress(doc *Doc) (data []byte, err error) {
	if nil == doc {
		return nil, nil
	}

	_, err = w.compressor.Write(doc.Content)
	if err != nil {
		return nil, err
	}
	w.comBuf.Reset()
	_ = w.compressor.Close()
	nd := &Doc{Key: doc.Key, Content: w.comBuf.Bytes()}
	w.buf.Reset()
	_, err = nd.writeDoc(w.buf)
	if err != nil {
		return nil, err
	}
	return w.buf.Bytes(), nil
}
