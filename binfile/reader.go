package binfile

import (
	"errors"
)

type BinReader interface {
	Close()
	ReadAt(offset int64, decompress bool) (*Doc, error)
	ReadDocs(opt *ReadOption)
	Count(offset int64, verboseStep uint32) (count uint32, err error)
	List(opt *ReadOption, keyOnly bool)
	Search(key string, offset int64) int64
	Next(offset int64) (pos int64, doc *Doc)
}

var InvalidDocumentFound = errors.New("invalid document found")

func NewBinReader(filename string, compressType int) BinReader {
	bf := newBinFile(filename, compressType)
	if bf == nil {
		return nil
	}
	return &docReader{*bf}
}

type ReadOption struct {
	Offset int64 `json:"start offset"`
	Limit  int32 `json:"number of document to read"`
	Step   int32 `json:"document read interval"`
}

type SearchOption struct {
	Key    string `json:"key to search"`
	Offset int64  `json:"start offset to search"`
}