package binfile

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"regexp"
	"time"
)

type docReader struct {
	binReaderFile
	fn *os.File
}

func (dr *docReader) Open() error {
	if dr.file != nil {
		return nil
	}
	fn, err := os.OpenFile(dr.filename, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	dr.fn = fn
	dr.file = fn
	return nil
}
func (dr *docReader) openAt(offset int64) (int64, error) {
	err := dr.Open()
	if err != nil {
		return 0, err
	}
	pos, err := dr.Seek(offset, io.SeekStart)
	if err != nil {
		return pos, err
	}
	return pos, nil
}

// ReadAt read doc at specified position
func (dr *docReader) ReadAt(offset int64, decompress bool) (doc *Doc, err error) {
	if _, err = dr.openAt(offset); err != nil {
		return nil, err
	}
	//pos, err := dr.Seek(0, io.SeekCurrent)
	//fmt.Printf("offset: %20d, current: %20d\n", offset, pos)
	return ReadDoc(dr.file, dr.compressType, decompress)
}

// ReadDocs doc at specified position
func (dr *docReader) ReadDocs(opt *ReadOption) {
	var err error
	if _, err = dr.openAt(opt.Offset); err != nil {
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
		err = dr.skipNext()
		if err != nil {
			break
		}
		count -= 1
	}
}

// Count how many documents in file start from offset
func (dr *docReader) Count(offset int64, nThreads int, verboseStep uint32) int64 {
	readSize, err := dr.openAt(offset)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "\nfile open error\n%v", err)
		return -1
	}

	if offset > readSize {
		return 0
	}

	if nThreads <= 1 {
		return simpleCount(dr.fn, offset, -1, 0, verboseStep)
	}
	workerReadSize := readSize / int64(nThreads)
	countCh := make(chan int64, nThreads)

	for no := 0; no < nThreads; no++ {
		go conCount(countCh, dr.filename, offset, offset+workerReadSize, dr.compressType, no, verboseStep)
		offset += workerReadSize
		if offset > readSize {
			offset = readSize
		}
	}
	total := int64(0)
	for no := 0; no < nThreads; no++ {
		cnt := <-countCh
		if cnt < 0 {
			total = -1
		} else {
			total += cnt
		}
	}
	return total
}

func newDocReader(filename string, compressType int) *docReader {
	bf := newBinReaderFile(filename, compressType, true)
	if nil == bf {
		return nil
	}
	return &docReader{binReaderFile: *bf}
}

func conCount(ch chan int64, fn string, start, end int64, ct, no int, verboseStep uint32) {
	br := newDocReader(fn, ct)
	pos, doc := br.Next(start)
	if doc != nil {
		count := simpleCount(br.fn, pos, end, no, verboseStep)
		ch <- count
	} else {
		ch <- 0
	}
}

func simpleCount(fs *os.File, start, end int64, no int, verboseStep uint32) (count int64) {
	count = 0
	curPos, err := fs.Seek(start, 0)
	//fmt.Printf("count fd: %d\n", fs.Fd())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "\n[%d]read doc error at %d\n%v", no, curPos, err)
		return -1
	}
	var nextVerbose = uint32(1)
	if Verbose {
		if end != -1 {
			fmt.Printf("[%d] count how many documents from position %d to %d\n", no, start, end)
		} else {
			fmt.Printf("[%d] count how many documents from position %d to end\n", no, start)
		}
	}
	for {
		err = skipOne(fs)
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		count++
		if Verbose && uint32(count) == nextVerbose {
			fmt.Printf("[%d] got %10d documents from %20d to position %20d\n", no, count, start, curPos)
			if verboseStep == 0 {
				nextVerbose = nextVerbose * 10
			} else {
				nextVerbose = nextVerbose + verboseStep
			}
		}
		curPos, err = fs.Seek(0, 1)
		if err == io.EOF || end >= 0 && curPos > end {
			break
		}
		if err != nil {
			break
		}
	}
	if err != nil && err != io.EOF {
		_, _ = fmt.Fprintf(os.Stderr, "\n[%d] read doc error at %d\n%v", no, curPos, err)
		return -1
	}
	return count
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
	_, _ = dr.Seek(int64(valueSize), io.SeekCurrent)
	return doc, nil
}

func (dr *docReader) resetOffset(offset int64) {
	_, _ = dr.Seek(offset, io.SeekStart)
}

// List documents in bin file
func (dr *docReader) List(opt *ReadOption, keyOnly bool) {
	docPos, err := dr.openAt(opt.Offset)
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
		curPos, _ := dr.Seek(0, io.SeekCurrent)
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
func (dr *docReader) Search(opt SearchOption) int64 {
	_, err := dr.openAt(opt.Offset)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return -1
	}
	reg, err := regexp.Compile(opt.Key)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "invalid key pattern %s, %v\n", opt.Key, err)
		return -1
	}
	var docPos int64 = -1
	var doc *DocKey
	var found int64 = -1
	skip := opt.Number
	if skip < 0 {
		rand.Seed(time.Now().Unix())
		skip = rand.Intn(100)
	}
	if Verbose {
		fmt.Printf("skip: %d\n", skip)
	}
	for {
		docPos, _ = dr.Seek(0, io.SeekCurrent)
		doc, err = dr.ReadKey()
		if err == io.EOF || doc == nil {
			break
		}

		if reg.MatchString(doc.Key) {
			found = docPos
			if skip > 0 {
				skip--
			} else {
				break
			}
		}
	}
	return found
}

func skipOne(fs *os.File) (err error) {
	var offset int64
	var size int32
	offset, err = fs.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, _ = fs.Seek(offset, io.SeekStart)
		}
	}()
	// read key size
	size, err = ReadKeySize(fs)
	if err != nil {

		return err
	}
	// skip to value size
	_, err = fs.Seek(int64(size), io.SeekCurrent)
	if err != nil {
		if err == io.EOF {
			err = InvalidDocumentFound
		}
		return err
	}
	// read value size
	size, err = ReadKeySize(fs)
	if err != nil {
		if err == io.EOF {
			err = InvalidDocumentFound
		}
		return err
	}
	// skip value bytes
	_, err = fs.Seek(int64(size), 1)
	if err == io.EOF {
		err = nil
	}
	return err
}

// skipNext skip next document
func (dr *docReader) skipNext() error {
	return skipOne(dr.fn)
}

// Next document position
func (dr *docReader) Next(offset int64) (pos int64, doc *Doc) {
	var err error
	_, err = dr.openAt(offset)
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

// Next document position
func (dr *docReader) next() (doc *Doc, err error) {
	doc, err = ReadDoc(dr.file, dr.compressType, true)
	if err == io.EOF {
		return nil, nil
	}
	return doc, err
}
