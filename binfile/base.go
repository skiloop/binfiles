package binfile

import "os"

type binFile struct {
	filename     string
	file         *os.File
	compressType int
}

func newBinFile(filename string, compressType int, checkExist bool) *binFile {
	if checkExist {
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			return nil
		}
	}
	return &binFile{filename: filename, compressType: compressType}
}

func (dr *binFile) open() (err error) {
	dr.file, err = os.Open(dr.filename)
	return err
}

func (dr *binFile) Close() {
	if dr.file != nil {
		_ = dr.file.Close()
		dr.file = nil
	}
}

func (dr *binFile) checkAndOpen() error {
	if dr.file == nil {
		return dr.open()
	}
	return nil
}
