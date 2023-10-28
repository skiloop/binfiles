package binfile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
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

type ReadOption struct {
	Offset int64 `json:"start offset"`
	Limit  int32 `json:"number of document to read"`
	Step   int32 `json:"document read interval"`
}

type SearchOption struct {
	Key    string `json:"key to search"`
	Offset int64  `json:"start offset to search"`
}

// ReadAt read doc at specified position
func (br BinReader) ReadAt(offset int64, decompress bool) (doc *Doc, err error) {
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

// ReadDocs doc at specified position
func (br BinReader) ReadDocs(opt *ReadOption) {
	var err error
	if _, err = br.openAndSeek(opt.Offset); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "open error: %v\n", err)
		return
	}
	var doc *Doc
	count := opt.Limit
	for {
		doc, err = ReadDoc(br.file, br.compressType, true)
		if err == io.EOF {
			break
		}
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "read doc error: %v\n", err)
			return
		}
		if Verbose {
			fmt.Printf("%-20s\t%s\n", doc.Key, doc.Content)
		} else {
			fmt.Println(doc.Content)
		}
		if opt.Limit > 0 {
			count -= 1
			if count <= 0 {
				break
			}
		}
		br.skipDocs(opt.Step)
	}
}

func (br BinReader) skipDocs(count int32) {
	var err error
	for count > 0 {
		err = br.seekNext()
		if err != nil {
			break
		}
		count -= 1
	}
}

// Count how many documents in file start from offset
func (br BinReader) Count(offset int64, verboseStep uint32) (count uint32, err error) {
	var curPos int64
	count = 0
	curPos, err = br.openAndSeek(offset)
	if err != nil {
		return count, err
	}
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
			if verboseStep == 0 {
				nextVerbose = nextVerbose * 10
			} else {
				nextVerbose = nextVerbose + verboseStep
			}
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

func (br BinReader) ReadKey() (doc *DocKey, err error) {
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

func (br BinReader) resetOffset(offset int64) {
	_, _ = br.file.Seek(offset, 0)
}

func (br BinReader) openAndSeek(offset int64) (int64, error) {
	var err error
	if err = br.checkAndOpen(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return -1, err
	}
	return br.file.Seek(offset, 0)
}

// List documents in bin file
func (br BinReader) List(opt *ReadOption, keyOnly bool) {
	docPos, err := br.openAndSeek(opt.Offset)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}
	var count int32
	var doc *DocKey
	count = 0
	for opt.Limit == 0 || count < opt.Limit {
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
		br.skipDocs(opt.Step)
	}
	if !keyOnly {
		fmt.Printf("total %d\n", count)
	}
}

// Search document in bin file
func (br BinReader) Search(key string, offset int64) int64 {
	_, err := br.openAndSeek(offset)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return -1
	}
	reg, err := regexp.Compile(key)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "invalid key pattern %s, %v\n", key, err)
		return -1
	}
	var docPos int64 = -1
	var doc *DocKey
	for {
		docPos, _ = br.file.Seek(0, 1)
		doc, err = br.ReadKey()
		if err == io.EOF || doc == nil {
			break
		}
		if reg.MatchString(doc.Key) {
			return docPos
		}
	}
	return -1
}

// seekNext seek next document
func (br BinReader) seekNext() (err error) {
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

func (br BinReader) open() (err error) {
	br.file, err = os.Open(br.filename)
	return err
}

func (br BinReader) Close() {
	if br.file != nil {
		_ = br.file.Close()
		br.file = nil
	}
}

func (br BinReader) checkAndOpen() error {
	if br.file == nil {
		err := br.open()
		if nil != err {
			return err
		}
	}
	return nil
}

// Next document position
func (br BinReader) Next(offset int64) (pos int64, doc *Doc) {
	var err error
	_, err = br.openAndSeek(offset)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return -1, nil
	}
	pos = offset
	for {
		br.resetOffset(pos)
		doc, err = ReadDoc(br.file, br.compressType, true)
		if err == io.EOF {
			return -1, nil
		}
		if err == nil {
			return pos, doc
		}
		if Debug {
			bytes := pos - offset
			if bytes < 1024 {
				fmt.Printf("%10d\t%10d bytes search\n", pos, bytes)
			} else {
				fmt.Printf("%10d\t%10dk search\n", pos, bytes)
			}
		}
		pos += 1
	}
}
