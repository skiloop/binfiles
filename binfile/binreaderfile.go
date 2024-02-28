package binfile

import (
	"io"
	"os"
)

type binReaderFile struct {
	binFile
	file io.ReadSeekCloser
}

func newBinReaderFile(filename string, compressType int, checkExist bool) *binReaderFile {
	if checkExist {
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			return nil
		}
	}
	bf := binFile{filename: filename, compressType: compressType}
	return &binReaderFile{binFile: bf}
}

func (dr *binReaderFile) Close() {
	if dr.file != nil {
		_ = dr.file.Close()
		dr.file = nil
	}
}

func (dr *binReaderFile) Seek(pos int64, whence int) (int64, error) {
	return dr.file.Seek(pos, whence)
}
