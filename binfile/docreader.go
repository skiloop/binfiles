package binfile

import (
	"io"
)

type DocReader interface {
	Read(decompress bool) (*Doc, error)
	CompressType() int
	Close() error
}

type docReader struct {
	r            io.Reader
	compressType int
}

func (dr *docReader) Close() error {
	if r, ok := dr.r.(io.Closer); ok {
		return r.Close()
	}
	return nil
}

func (dr *docReader) CompressType() int {
	return dr.compressType
}

// ReadAt read doc at specified position
func (dr *docReader) Read(decompress bool) (doc *Doc, err error) {
	doc = new(Doc)
	_, err = ReadDoc(dr.r, doc)
	if err != nil {
		return nil, err
	}
	if decompress {
		return Decompress(doc, dr.compressType)
	}
	return doc, nil
}

func NewDocReader(reader io.Reader, compressType int) DocReader {
	return &docReader{r: reader, compressType: compressType}
}
