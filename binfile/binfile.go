package binfile

import (
	"os"
	"io"
	"fmt"
)

type BinReader struct {
	filename     string
	file         *os.File
	compressType int
}

func NewBinReader(filename string, compressType int) (*BinReader) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil
	}
	return &BinReader{filename: filename, compressType: compressType}
}

/**
 read bin file at position pos
 */
func (br *BinReader) ReadAt(pos int64, decompress bool) (doc *Doc, err error) {
	if err := br.checkAndOpen(); err != nil {
		return nil, err
	}
	_, err = br.file.Seek(pos, 0)
	if err != nil {
		return nil, err
	}
	return ReadDoc(br.file, br.compressType, decompress)
}

func (br *BinReader) Count(from int64, writer io.Writer) error {
	if err := br.checkAndOpen(); err != nil {
		return err
	}
	_, err := br.file.Seek(from, 0)
	if err != nil {
		return err
	}
	if writer == nil {
		writer = os.Stdin
	}
	var count int
	for {
		doc, err := ReadDoc(br.file, br.compressType, false)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		curPos, _ := br.file.Seek(0, 1)
		count++
		writer.Write([]byte(fmt.Sprintf("[%d]\t%20d\t%s\n", count, curPos, doc.Key)))
	}
	return nil
}

func (br *BinReader) open() (err error) {
	br.file, err = os.Open(br.filename)
	return err
}

func (br *BinReader) Close() {
	if br.file != nil {
		br.file.Close()
		br.file = nil
	}
}

func (br *BinReader) checkAndOpen() error {
	if br.file == nil {
		err := br.open()
		if nil != err {
			return err
		}
	}
	return nil
}
