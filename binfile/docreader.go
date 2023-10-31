package binfile

import (
	"fmt"
	"io"
	"os"
	"regexp"
)

type docReader struct {
	binFile
}

// ReadAt read doc at specified position
func (dr *docReader) ReadAt(offset int64, decompress bool) (doc *Doc, err error) {
	if err = dr.checkAndOpen(); err != nil {
		return nil, err
	}
	_, err = dr.file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	//pos, err := dr.file.Seek(0, 1)
	//fmt.Printf("offset: %20d, current: %20d\n", offset, pos)
	return ReadDoc(dr.file, dr.compressType, decompress)
}

// ReadDocs doc at specified position
func (dr *docReader) ReadDocs(opt *ReadOption) {
	var err error
	if _, err = dr.openAndSeek(opt.Offset); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "open error: %v\n", err)
		return
	}
	var doc *Doc
	count := opt.Limit
	for {
		doc, err = ReadDoc(dr.file, dr.compressType, true)
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
		dr.skipDocs(opt.Step)
	}
}

func (dr *docReader) skipDocs(count int32) {
	var err error
	for count > 0 {
		err = dr.seekNext()
		if err != nil {
			break
		}
		count -= 1
	}
}

// Count how many documents in file start from offset
func (dr *docReader) Count(offset int64, verboseStep uint32) (count uint32, err error) {
	var curPos int64
	count = 0
	curPos, err = dr.openAndSeek(offset)
	if err != nil {
		return count, err
	}
	var nextVerbose = count + 1
	if Verbose {
		fmt.Printf("count how many documents from position %d\n", offset)
	}
	for {
		err = dr.seekNext()
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
		curPos, err = dr.file.Seek(0, 1)
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, err
		}
	}
	return count, nil
}

func (dr *docReader) ReadKey() (doc *DocKey, err error) {
	var keySize int32

	keySize, err = ReadKeySize(dr.file)
	if err != nil {
		return nil, err
	}
	doc = new(DocKey)
	doc.KeySize = keySize
	var keyBuf []byte
	keyBuf, err = readBytes(dr.file, int(keySize))
	if err != nil {
		return nil, err
	}
	doc.Key = string(keyBuf)
	valueSize, err := ReadKeySize(dr.file)
	if err != nil {
		return nil, err
	}
	doc.ContentSize = valueSize
	_, _ = dr.file.Seek(int64(valueSize), 1)
	return doc, nil
}

func (dr *docReader) resetOffset(offset int64) {
	_, _ = dr.file.Seek(offset, 0)
}

func (dr *docReader) openAndSeek(offset int64) (int64, error) {
	var err error
	if err = dr.checkAndOpen(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return -1, err
	}
	return dr.file.Seek(offset, 0)
}

// List documents in bin file
func (dr *docReader) List(opt *ReadOption, keyOnly bool) {
	docPos, err := dr.openAndSeek(opt.Offset)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}
	var count int32
	var doc *DocKey
	count = 0
	for opt.Limit == 0 || count < opt.Limit {
		doc, err = dr.ReadKey()
		if err == io.EOF || doc == nil {
			break
		}
		curPos, _ := dr.file.Seek(0, 1)
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
		dr.skipDocs(opt.Step)
	}
	if !keyOnly {
		fmt.Printf("total %d\n", count)
	}
}

// Search document in bin file
func (dr *docReader) Search(key string, offset int64) int64 {
	_, err := dr.openAndSeek(offset)
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
		docPos, _ = dr.file.Seek(0, 1)
		doc, err = dr.ReadKey()
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
func (dr *docReader) seekNext() (err error) {
	var offset int64
	var size int32
	offset, err = dr.file.Seek(0, 1)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			dr.resetOffset(offset)
		}
	}()
	// read key size
	size, err = ReadKeySize(dr.file)
	if err != nil {

		return err
	}
	// skip to value size
	_, err = dr.file.Seek(int64(size), 1)
	if err != nil {
		if err == io.EOF {
			err = InvalidDocumentFound
		}
		return err
	}
	// read value size
	size, err = ReadKeySize(dr.file)
	if err != nil {
		if err == io.EOF {
			err = InvalidDocumentFound
		}
		return err
	}
	// skip value bytes
	_, err = dr.file.Seek(int64(size), 1)
	if err == io.EOF {
		err = nil
	}
	return err
}

// Next document position
func (dr *docReader) Next(offset int64) (pos int64, doc *Doc) {
	var err error
	_, err = dr.openAndSeek(offset)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return -1, nil
	}
	pos = offset
	for {
		dr.resetOffset(pos)
		doc, err = ReadDoc(dr.file, dr.compressType, true)
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
