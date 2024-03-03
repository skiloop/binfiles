package binfile

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"regexp"
	"time"
)

type BinReader interface {
	Close()
	Read(offset int64, decompress bool) (*Doc, error)
	ReadDocs(opt *ReadOption)
	Count(offset int64, nThreads int, verboseStep uint32) int64
	List(opt *ReadOption, keyOnly bool)
	Search(opt SearchOption) int64
	Next(offset int64) (pos int64, doc *Doc)
}

var InvalidDocumentFound = errors.New("invalid document found")

func NewBinReader(filename string, compressType int) (BinReader, error) {
	fn, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to open %s: %v\n", filename, err)
		return nil, err
	}
	ds := NewSeeker(fn, compressType)
	return &binReader{filename: filename, file: fn, docSeeker: ds}, nil
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

func NewFileReader(filename string, compressType int) (*os.File, DocReader, error) {

	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, nil, err
	}
	fn, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, nil, err
	}
	dr := NewDocReader(fn, compressType)
	return fn, dr, nil
}

type binReader struct {
	filename  string
	file      *os.File
	docSeeker SeekReader
}

func (br *binReader) Close() {
	if br.file != nil {
		_ = br.file.Close()
	}
}

func (br *binReader) Read(offset int64, decompress bool) (*Doc, error) {
	return br.docSeeker.ReadAt(offset, decompress)
}

func (br *binReader) close() {
	if br.file != nil {
		_ = br.file.Close()
		br.file = nil
	}
}

// ReadDocs doc at specified position
func (br *binReader) ReadDocs(opt *ReadOption) {
	var err error
	var doc *Doc
	count := opt.Limit
	offset := opt.Offset
	for {
		doc, err = br.Read(offset, true)
		if err == io.EOF {
			break
		}
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "read doc error: %v\n", err)
			return
		}
		if Verbose {
			fmt.Printf("%-20s\t%s\n", string(doc.Key), string(doc.Content))
		} else {
			fmt.Println(string(doc.Content))
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

func (br *binReader) skipDocs(count int32) {
	var err error
	for count > 0 {
		err = br.skipNext()
		if err != nil {
			break
		}
		count -= 1
	}
}

// Count how many documents in file start from offset
func (br *binReader) Count(offset int64, nThreads int, verboseStep uint32) int64 {
	readSize, err := br.docSeeker.Seek(offset, io.SeekStart)
	if err != nil {
		return -1
	}
	if nThreads <= 1 {
		return br.simpleCount(readSize, -1, 0, verboseStep)
	}
	workerReadSize := readSize / int64(nThreads)
	countCh := make(chan int64, nThreads)

	for no := 0; no < nThreads; no++ {
		go br.conCount(countCh, offset, offset+workerReadSize, no, verboseStep)
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

// count concurrently
func (br *binReader) conCount(ch chan int64, start, end int64, no int, verboseStep uint32) {

	brd, err := NewBinReader(br.filename, br.docSeeker.CompressType())
	if err != nil {
		ch <- 0
		return
	}
	dr, ok := brd.(*binReader)

	if !ok {
		ch <- 0
		return
	}
	ch <- dr.simpleCount(start, end, no, verboseStep)
}

func (br *binReader) simpleCount(start, end int64, no int, verboseStep uint32) (count int64) {
	count = 0
	curPos, err := br.docSeeker.Seek(start, io.SeekCurrent)
	if err != nil {
		return count
	}
	var nextVerbose = uint32(1)
	if Verbose {
		if end != -1 {
			fmt.Printf("[%d] count how many documents from position %d to %d\n", no, start, end)
		} else {
			fmt.Printf("[%d] count how many documents from position %d to workerEndFlag\n", no, start)
		}
	}
	for {
		err = br.skipNext()
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
		curPos, err = br.docSeeker.Seek(0, 1)
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

func (br *binReader) resetOffset(offset int64) (err error) {
	_, err = br.docSeeker.Seek(offset, io.SeekStart)
	return err
}

// List documents in bin file
func (br *binReader) List(opt *ReadOption, keyOnly bool) {
	docPos, err := br.docSeeker.Seek(opt.Offset, io.SeekStart)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}
	var count int32
	doc := &DocKey{}
	count = 0
	for opt.Limit == 0 || count < opt.Limit {
		_, err = br.docSeeker.ReadKey(doc)
		if err == io.EOF || doc == nil {
			break
		}
		curPos, _ := br.docSeeker.Seek(0, io.SeekCurrent)
		count++
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[!%d]\t%20d\t%v\n", count, docPos, err)
			return
		}
		var msg string
		if keyOnly {
			msg = string(doc.Key)
		} else {
			msg = fmt.Sprintf("[%d]\t%20d\t%s", count, docPos, string(doc.Key))
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
func (br *binReader) Search(opt SearchOption) int64 {
	_, err := br.docSeeker.Seek(opt.Offset, io.SeekStart)
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

	var found int64 = -1
	skip := opt.Number
	if skip < 0 {
		rnd := rand.New(rand.NewSource(time.Now().Unix()))
		skip = rnd.Intn(100)
	}
	if Verbose {
		fmt.Printf("skip: %d\n", skip)
	}
	doc := &DocKey{}
	for {
		docPos, _ = br.docSeeker.Seek(0, io.SeekCurrent)
		_, err = br.docSeeker.ReadKey(doc)
		if err == io.EOF || doc == nil {
			break
		}

		if reg.MatchString(string(doc.Key)) {
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

func (br *binReader) skipNext() (err error) {
	var offset int64
	offset, err = br.docSeeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, _ = br.docSeeker.Seek(offset, io.SeekStart)
		}
	}()
	// read key size
	dk := &DocKey{}
	var n int
	n, err = br.docSeeker.ReadKey(dk)
	offset += int64(n)
	if err != nil {
		return err
	}
	return err
}

// Next document position
func (br *binReader) Next(offset int64) (pos int64, doc *Doc) {
	var err error
	pos = offset
	for {
		err = br.resetOffset(pos)
		if err == io.EOF {
			return -1, nil
		}
		doc, err = br.docSeeker.Read(true)
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
