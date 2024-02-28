package binfile

import (
	"errors"
)

type BinReader interface {
	Close()
	ReadAt(offset int64, decompress bool) (*Doc, error)
	ReadDocs(opt *ReadOption)
	Count(offset int64, nThreads int, verboseStep uint32) int64
	List(opt *ReadOption, keyOnly bool)
	Search(opt SearchOption) int64
	Next(offset int64) (pos int64, doc *Doc)
}

var InvalidDocumentFound = errors.New("invalid document found")

func NewBinReader(filename string, compressType int) BinReader {
	bf := newBinReaderFile(filename, compressType, true)
	if bf == nil {
		return nil
	}
	return &docReader{binReaderFile: *bf}
}

type ReadOption struct {
	Offset int64 `json:"start offset"`
	Limit  int32 `json:"number of document to read"`
	Step   int32 `json:"document read interval"`
}

type SearchOption struct {
	Key    string `json:"key to search"`
	Number int    `json:"skip the n of found docs. If less then n docs found then return last one"`
	Offset int64  `json:"start offset to search"`
}
