package binfile

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"regexp"
	"time"
	"unsafe"
)

type SeekOption struct {
	Offset  int64
	Pattern string
	KeySize int
	DocSize int
	End     int64
}
type BinReader interface {
	Close()
	Read(offset int64, decompress bool) (*Doc, error)
	ReadDocs(opt *ReadOption)
	Count(offset int64, nThreads int, verboseStep uint32, skipError bool) int64
	List(opt *ReadOption, keyOnly bool)
	Search(opt SearchOption) int64
	// Next seek for next doc
	Next(opt *SeekOption) (pos int64, doc *Doc)
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
	Offset      int64  `help:"start offset"`
	Limit       int32  `help:"number of document to read, -1 to read all"`
	Step        int32  `help:"document read interval"`
	OutCompress int    `help:"output compress mode, only works when output not empty"`
	Output      string `help:"output filename"`
	Repack      bool   `help:"repack"`
	SkipError   bool   `help:"skip error"`
}

type SearchOption struct {
	Key       string `json:"key to search"`
	Number    int    `json:"skip the n of found docs. If less then n docs found then return last one"`
	Offset    int64  `json:"start offset to search"`
	SkipError bool   `json:"continue searching when encounter doc error"`
}

//
//func NewFileReader(filename string, compressType int) (*os.File, DocReader, error) {
//
//	if _, err := os.Stat(filename); os.IsNotExist(err) {
//		return nil, nil, err
//	}
//	fn, err := os.OpenFile(filename, os.O_RDONLY, 0644)
//	if err != nil {
//		return nil, nil, err
//	}
//	dr := NewDocReader(fn, compressType)
//	return fn, dr, nil
//}

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
	var doc *Doc
	w, err := newOutWriter(opt.Output, opt.OutCompress)
	if err != nil {
		errorf("%v", err)
		return
	}
	defer closeWriter(w, "close output")
	offset := opt.Offset
	err = br.resetOffset(offset)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "seek position error: %v\n", err)
		return
	}
	count := opt.Limit
	if opt.Output != "" {
		defer func() {
			if err != nil {
				off, er := br.docSeeker.Seek(0, io.SeekCurrent)
				if er == nil {
					_, _ = fmt.Fprintf(os.Stderr, "last read postion: %d\n", off)
				}
			}
		}()
	}

	for {
		offset, _ = br.docSeeker.Seek(0, io.SeekCurrent)
		doc, err = br.docSeeker.Read(true)
		if err == io.EOF {
			break
		}
		if err != nil {
			if opt.Limit == 1 || !opt.SkipError {
				_, _ = fmt.Fprintf(os.Stderr, "read doc error: %v\n", err)
				return
			}
			pos, dc := br.next(offset, -1, -1, -1, nil)
			if dc == nil {
				_, _ = fmt.Fprintf(os.Stderr, "read doc error: %v\n", err)
				return
			}
			_, _ = fmt.Fprintf(os.Stderr, "fail to read doc at %d, skipped, error: %v\n", offset, err)
			offset, doc = pos, dc
		}
		if Verbose {
			_, _ = fmt.Fprintf(w, "%-20s\t%s\n", string(doc.Key), string(doc.Content))
		} else {
			_, err = fmt.Fprintln(w, string(doc.Content))
		}
		if (err == nil || !opt.SkipError) && opt.Limit > 0 {
			count -= 1
			if count <= 0 {
				break
			}
		}
		if err = br.skipDocs(opt.Step); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "skip docs error: %v\n", err)
			break
		}
	}
}

// skipDocs skip next N valid docs
func (br *binReader) skipDocs(count int32) (err error) {
	for count > 0 {
		err = br.skipNext()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			_, doc := br.next(-1, -1, -1, -1, nil)
			if doc == nil {
				break
			}
		}
		count -= 1
	}
	return
}

// Count how many documents in file start from offset
func (br *binReader) Count(offset int64, nThreads int, verboseStep uint32, skipError bool) int64 {

	if nThreads <= 1 {
		return br.simpleCount(offset, -1, 0, verboseStep, skipError)
	}
	remainSize, err := br.docSeeker.Seek(offset, io.SeekEnd)
	if err != nil {
		return -1
	}
	workerReadSize := remainSize / int64(nThreads)
	countCh := make(chan int64, nThreads)
	start := offset
	for no := 0; no < nThreads; no++ {
		go br.conCount(countCh, start, start+workerReadSize, no, verboseStep, skipError)
		start += workerReadSize
		if start-offset > remainSize {
			break
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
func (br *binReader) conCount(ch chan int64, start, end int64, no int, verboseStep uint32, skipError bool) {

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
	ch <- dr.simpleCount(start, end, no, verboseStep, skipError)
}

func (br *binReader) simpleCount(start, end int64, no int, verboseStep uint32, skipError bool) (count int64) {
	count = 0
	curPos, doc := br.Next(&SeekOption{
		Offset:  start,
		Pattern: "",
		KeySize: int(KeySizeLimit),
		DocSize: MaxDocSize,
		End:     end,
	})
	if doc == nil {
		return count
	}
	var nextVerbose = uint32(1)
	if Verbose {
		if end != -1 {
			fmt.Printf("[%d] count how many documents from position %d to %d\n", no, start, end)
		} else {
			fmt.Printf("[%d] count how many documents from position %d to end\n", no, start)
		}
		fmt.Printf("[%d] start doc position: %d\n", no, curPos)
	}
	var err error
	count += 1
	for {
		curPos, _ = br.current()
		err = br.skipNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			if !skipError {
				break
			}
			pos, dc := br.next(curPos, -1, -1, -1, nil)
			if dc == nil {
				break
			}
			_ = br.resetOffset(pos)
			continue
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
		curPos, err = br.current()
		if err == io.EOF || end >= 0 && curPos >= end {
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
	if Verbose {
		fmt.Printf("[%d] got %10d documents from %20d to position %20d\n", no, count, start, curPos)
	}
	return count
}

func (br *binReader) resetOffset(offset int64) (err error) {
	_, err = br.docSeeker.Seek(offset, io.SeekStart)
	return err
}

func (br *binReader) current() (pos int64, err error) {
	return br.docSeeker.Seek(0, io.SeekCurrent)
}

// List documents in bin file
func (br *binReader) List(opt *ReadOption, keyOnly bool) {
	current, err := br.docSeeker.Seek(opt.Offset, io.SeekStart)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}
	var count int32
	doc := &DocKey{}
	count = 0

	for opt.Limit == 0 || count < opt.Limit {
		current, _ = br.current()
		_, err = br.docSeeker.ReadKey(doc)
		if err == io.EOF || doc == nil {
			break
		}
		if err != nil {
			if !opt.SkipError {
				_, _ = fmt.Fprintf(os.Stderr, "[!%d]\t%20d\t%v\n", count, current, err)
				return
			}
			pos, document := br.next(current, -1, -1, -1, nil)
			if document == nil {
				_, _ = fmt.Fprintf(os.Stderr, "[!%d]\t%20d\t%v\n", count, current, err)
				return
			}
			_ = br.resetOffset(pos)
			continue
		}
		count++
		var msg string
		if keyOnly {
			msg = string(doc.Key)
		} else {
			msg = fmt.Sprintf("[%d]\t%20d\t%s", count, current, string(doc.Key))
		}
		fmt.Println(msg)
		_ = br.skipDocs(opt.Step)
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
		if err == io.EOF {
			break
		}
		if err != nil {
			pos, dc := br.next(docPos, -1, -1, -1, nil)
			if dc == nil {
				break
			}
			docPos, doc = pos, &DocKey{Key: dc.Key, KeySize: int32(len(dc.Key)), ContentSize: int32(len(dc.Content))}
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

// skipNext skip next doc, include invalid doc
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
func (br *binReader) next(start, end int64, keySize, docSize int, regex *regexp.Regexp) (pos int64, doc *Doc) {

	pos = start
	if keySize <= 0 {
		keySize = int(KeySizeLimit)
	}
	err := br.resetOffset(pos)
	if err == io.EOF {
		return -1, nil
	}
	buff := make([]byte, int(unsafe.Sizeof(int32(0)))*2+int(KeySizeLimit))

	_, err = br.file.Read(buff)
	if err != nil {
		//TODO: doc size not larger than len(buff)
		if err == io.EOF {
			return 0, nil
		}
		_, _ = fmt.Fprintf(os.Stderr, "read file error: %v\n", err)
		return 0, nil
	}
	var dk *DocKey
	for {
		dk, err = br.checkKey(buff, regex, keySize, docSize)
		if err != nil {
			break
		}
		if dk != nil {
			doc, err = br.docSeeker.ReadAt(pos, true)
			if doc != nil {
				break
			}
			_, _ = br.file.Seek(pos+int64(len(buff)), io.SeekStart)
		}
		buff, err = br.readByte(buff)
		if err != nil {
			break
		}
		pos += 1
		if Debug {
			nBytes := pos - start
			if nBytes < 1024 {
				fmt.Printf("%10d\t%10d nBytes search\n", pos, nBytes)
			} else {
				fmt.Printf("%10d\t%10dk search\n", pos, nBytes)
			}
		}
		pos += 1
		if end > 0 && pos > end {
			break
		}
	}
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "seek next error: %v\n", err)
		return -1, nil
	}
	return pos, doc
}

// Next document position
func (br *binReader) Next(opt *SeekOption) (pos int64, doc *Doc) {
	var err error
	var regex *regexp.Regexp = nil
	if opt.Pattern != "" {
		regex, err = regexp.Compile(opt.Pattern)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "regex error: %v\n", err)
			return 0, nil
		}
	}
	return br.next(opt.Offset, opt.End, opt.KeySize, opt.DocSize, regex)
}

func (br *binReader) checkKey(buff []byte, pattern *regexp.Regexp,
	keyLimit, contentLimit int) (*DocKey, error) {
	var size int32
	r := bytes.NewBuffer(buff)
	_, _ = readInt32(r, &size)
	ks := size + 4
	if size <= 0 || size > int32(len(buff)-8) || keyLimit > 0 && int32(keyLimit) < size ||
		pattern != nil && !pattern.MatchString(string(buff[4:ks])) {
		return nil, nil
	}
	doc := &DocKey{
		KeySize:     size,
		ContentSize: 0,
		Key:         buff[4:ks],
	}
	ks += 4
	if ks < int32(len(buff)) {
		r = bytes.NewBuffer(buff[ks:])
		_, _ = readInt32(r, &size)
		if size < 0 || contentLimit > 0 && int32(contentLimit) < size {
			return nil, nil
		}
		doc.ContentSize = size
		return doc, nil
	}
	buff = append(buff, make([]byte, int(ks)-len(buff))...)
	_, err := br.file.Read(buff[ks:])
	if err != nil {
		return nil, err
	}
	_, _ = readInt32(r, &size)
	if contentLimit > 0 && int32(contentLimit) < size {
		_, _ = br.file.Seek(-4, io.SeekCurrent)
		return nil, nil
	}
	doc.ContentSize = size
	return doc, nil
}
func (br *binReader) readByte(buff []byte) ([]byte, error) {
	buff = buff[1:]
	buff = append(buff, make([]byte, 1)...)
	_, err := br.file.Read(buff[:1])
	if err != nil {
		return nil, err
	}
	return buff, nil
}
