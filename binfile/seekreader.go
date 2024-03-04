package binfile

import (
	"io"
)

type SeekReader interface {
	DocReader
	io.Seeker
	ReadAt(offset int64, decompress bool) (doc *Doc, err error)
	ReadKey(doc *DocKey) (nr int, err error)
}

type seekReader struct {
	DocReader
	rs io.ReadSeeker
}

func (sr *seekReader) Seek(offset int64, whence int) (int64, error) {
	return sr.rs.Seek(offset, whence)
}

func NewSeeker(rs io.ReadSeeker, compressType int) SeekReader {
	dr := NewDocReader(rs, compressType)
	return &seekReader{DocReader: dr, rs: rs}
}

// ReadAt read doc at specified position
func (sr *seekReader) ReadAt(offset int64, decompress bool) (doc *Doc, err error) {
	_, err = sr.rs.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	return sr.Read(decompress)
}

func (sr *seekReader) ReadKey(doc *DocKey) (n int, err error) {
	n, err = readHeader(sr.rs, doc)
	if err != nil {
		return n, err
	}
	_, err = sr.Seek(int64(doc.ContentSize), io.SeekCurrent)
	if err != nil {
		return n, err
	}
	return int(doc.ContentSize) + n, nil
}
