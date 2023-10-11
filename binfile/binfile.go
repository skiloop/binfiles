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
		if Verbose && count == nextVerbose {
			fmt.Printf("got %10d documents from %20d to position %20d\n", count, offset, curPos)
			nextVerbose = nextVerbose * 10
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

func (br *BinReader) ReadKey() (doc *DocKey, err error) {
	var keySize int32

	keySize, err = ReadKeySize(br.file)
	if err != nil {
		return nil, err
	}
	doc = new(DocKey)
	doc.KeySize = keySize
	var keyBuf []byte
	keyBuf, err = readBytes(br.file, int(keySize))
	if err != nil {
		return nil, err
	}
	doc.Key = string(keyBuf)
	valueSize, err := ReadKeySize(br.file)
	if err != nil {
		return nil, err
	}
	doc.ContentSize = valueSize
	_, _ = br.file.Seek(int64(valueSize), 1)
	return doc, nil
}

func (br *BinReader) resetOffset(offset int64) {
	_, _ = br.file.Seek(offset, 0)
}

// List documents in bin file
func (br *BinReader) List(offset int64, keyOnly bool) {
	var err error
	if err = br.checkAndOpen(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}
	docPos, err := br.file.Seek(offset, 0)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}
	var count int
	var doc *DocKey
	for {
		doc, err = br.ReadKey()
		if err == io.EOF || doc == nil {
			break
		}
		curPos, _ := br.file.Seek(0, 1)
		count++
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[!%d]\t%20d\t%v\n", count, docPos, err)
			return
		}
		var msg string
		if keyOnly {
			msg = doc.Key
		} else {
			msg = fmt.Sprintf("[%d]\t%20d\t%s", count, docPos, doc.Key)
		}
		fmt.Println(msg)
		docPos = curPos
	}
	if !keyOnly {
		fmt.Printf("total %d\n", count)
	}
}

func (br *BinReader) seekNext() (err error) {
	var offset int64
	var size int32
	offset, err = br.file.Seek(0, 1)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			br.resetOffset(offset)
		}
	}()
	// read key size
	size, err = ReadKeySize(br.file)
	if err != nil {

		return err
	}
	// skip to value size
	_, err = br.file.Seek(int64(size), 1)
	if err != nil {
		if err == io.EOF {
			err = InvalidDocumentFound
		}
		return err
	}
	// read value size
	size, err = ReadKeySize(br.file)
	if err != nil {
		if err == io.EOF {
			err = InvalidDocumentFound
		}
		return err
	}
	// skip value bytes
	_, err = br.file.Seek(int64(size), 1)
	if err == io.EOF {
		err = nil
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
