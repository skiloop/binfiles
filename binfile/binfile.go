package binfile

import (
	"errors"
	"fmt"
	"io"
	"os"
)

type BinReader struct {
	filename     string
	file         *os.File
	compressType int
}

var InvalidDocumentFound = errors.New("invalid document found")

func NewBinReader(filename string, compressType int) *BinReader {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil
	}
	return &BinReader{filename: filename, compressType: compressType}
}

// ReadAt read doc at specified position
func (br *BinReader) ReadAt(offset int64, decompress bool) (doc *Doc, err error) {
	if err = br.checkAndOpen(); err != nil {
		return nil, err
	}
	_, err = br.file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	//pos, err := br.file.Seek(0, 1)
	//fmt.Printf("offset: %20d, current: %20d\n", offset, pos)
	return ReadDoc(br.file, br.compressType, decompress)
}

// Count how many documents in file start from offset
func (br *BinReader) Count(offset int64) (count uint64, err error) {
	count = 0
	if err = br.checkAndOpen(); err != nil {
		return count, err
	}
	_, err = br.file.Seek(offset, 0)
	if err != nil {
		return count, err
	}
	var curPos = offset
	var nextVerbose = count + 1
	if Verbose {
		fmt.Printf("count how many documents from position %d\n", offset)
	}
	for {
		err = br.seekNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "\nread doc error at %d\n%v", curPos, err)
			return count, err
		}
		count++
		if Verbose && nextVerbose == nextVerbose {
			fmt.Printf("got %d documents at %d from position %d\n", count, curPos, offset)
			nextVerbose = nextVerbose + 1
		}
		curPos, err = br.file.Seek(0, 1)
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, err
		}
	}
	return count, nil
}

// List doc in bin file
func (br *BinReader) List(offset int64, writer io.Writer) error {
	if err := br.checkAndOpen(); err != nil {
		return err
	}
	docPos, err := br.file.Seek(offset, 0)
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

		curPos, _ := br.file.Seek(0, 1)
		count++
		if err != nil {
			writer.Write([]byte(fmt.Sprintf("[!%d]\t%20d\t%s\n", count, docPos, err.Error())))
			return err
		}
		writer.Write([]byte(fmt.Sprintf("[%d]\t%20d\t%s\n", count, docPos, doc.Key)))
		docPos = curPos
	}
	writer.Write([]byte(fmt.Sprintf("total %d\n", count)))
	return nil
}

func (br *BinReader) seekNext() (err error) {
	var offset int64
	var size int32
	offset, err = br.file.Seek(0, 1)
	if err != nil {
		return err
	}
	// read key size
	size, err = ReadKeySize(br.file)
	if err != nil {
		_, _ = br.file.Seek(offset, 0)
		return err
	}
	// skip to value size
	_, err = br.file.Seek(int64(size), 1)
	if err != nil {
		_, _ = br.file.Seek(offset, 0)
		if err == io.EOF {
			return InvalidDocumentFound
		}
		return err
	}
	// read value size
	size, err = ReadKeySize(br.file)
	if err != nil {
		_, _ = br.file.Seek(offset, 0)
		if err == io.EOF {
			return InvalidDocumentFound
		}
		return err
	}
	// skip value bytes
	_, err = br.file.Seek(int64(size), 1)
	if err == io.EOF {
		return nil
	}
	return err
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
