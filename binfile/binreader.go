package binfile

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"regexp"
	"time"
	"unsafe"
)

type SeekOption struct {
	Offset     int64
	End        int64
	Pattern    string
	KeySize    int
	DocSize    int
	Decompress bool
}

// CountOption is the option for counting documents
type CountOption struct {
	Offset      int64
	End         int64
	WorkerCount int
	VerboseStep uint32
	Input       string
	Pattern     string
	KeyOnly     bool
	SkipError   bool
}
type BinReader interface {
	Close()
	Read(offset int64, decompress bool) (*Doc, error)
	ReadDocs(opt *ReadOption)
	Count(opt *CountOption) int64
	List(opt *ReadOption, keyOnly bool)
	Search(opt SearchOption) int64
	// Next seek for next doc
	Next(opt *SeekOption) (pos int64, doc *Doc)
}

var ErrInvalidDocument = errors.New("invalid document found")

func NewBinReader(filename string, compressType int) (BinReader, error) {
	fn, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		LogError("failed to open %s: %v\n", filename, err)
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
	KeyPattern  string `help:"key pattern for key searching, regex supported, default: empty"`

	Repack    bool `help:"repack"`
	SkipError bool `help:"skip error"`
}

type SearchOption struct {
	Key       string `help:"key to search"`
	Skip      int    `help:"skip the first n docs found, if less then n docs found then return last one, 0 means no skip, negative means random skip"`
	Offset    int64  `help:"start offset to search"`
	SkipError bool   `help:"continue searching when encounter doc error"`
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

// func (br *binReader) Close() {
// 	if br.file != nil {
// 		_ = br.file.Close()
// 	}
// }

func (br *binReader) Read(offset int64, decompress bool) (*Doc, error) {
	return br.docSeeker.ReadAt(offset, decompress)
}

func (br *binReader) Close() {
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
		LogError("seek position error: %v\n", err)
		return
	}
	count := opt.Limit
	if opt.Output != "" {
		defer func() {
			if err != nil {
				off, er := br.docSeeker.Seek(0, io.SeekCurrent)
				if er == nil {
					LogError("last read postion: %d\n", off)
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
				LogError("read doc error: %v\n", err)
				return
			}
			pos, dc := br.next(offset, -1, -1, -1, nil, true)
			if dc == nil {
				LogError("read doc error: %v\n", err)
				return
			}
			LogError("fail to read doc at %d, skipped, error: %v\n", offset, err)
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
			LogError("skip docs error: %v\n", err)
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
			_, doc := br.next(-1, -1, -1, -1, nil, false)
			if doc == nil {
				break
			}
		}
		count -= 1
	}
	return
}

// Count how many documents in file start from offset
func (br *binReader) Count(opt *CountOption) int64 {

	if opt.WorkerCount <= 1 {
		return br.simpleCount(opt.Offset, opt.End, 0, opt.VerboseStep, opt.KeyOnly, opt.SkipError, opt.Pattern)
	}
	remainSize, err := br.docSeeker.Seek(opt.Offset, io.SeekEnd)
	if err != nil {
		return -1
	}
	if opt.End > 0 {
		remainSize = int64(math.Min(float64(remainSize), float64(opt.End-opt.Offset)))
	}
	workerReadSize := remainSize / int64(opt.WorkerCount)
	countCh := make(chan int64, opt.WorkerCount)
	start := opt.Offset
	for no := 0; no < opt.WorkerCount; no++ {
		go br.conCount(countCh, start, start+workerReadSize, no, opt.VerboseStep,
			opt.KeyOnly, opt.SkipError, opt.Pattern)
		start += workerReadSize
		if start-opt.Offset > remainSize {
			break
		}
	}
	total := int64(0)
	for no := 0; no < opt.WorkerCount; no++ {
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
func (br *binReader) conCount(ch chan int64, start, end int64, no int, verboseStep uint32,
	keyOnly bool, skipError bool, pattern string) {
	// TODO: fix concurrent count error: count mismatch
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
	ch <- dr.simpleCount(start, end, no, verboseStep, keyOnly, skipError, pattern)
}

func (br *binReader) simpleCount(start, end int64, no int, verboseStep uint32, keyOnly bool,
	skipError bool, pattern string) (count int64) {
	count = 0
	curPos, doc := br.Next(&SeekOption{
		Offset:     start,
		Pattern:    pattern,
		KeySize:    int(KeySizeLimit),
		DocSize:    MaxDocSize,
		End:        end,
		Decompress: !keyOnly,
	})
	if doc == nil {
		return count
	}
	var nextVerbose = verboseStep

	if Verbose {
		if end != -1 {
			LogInfo("[%d] count how many documents from position %d to %d\n", no, start, end)
		} else {
			LogInfo("[%d] count how many documents from position %d to end\n", no, start)
		}
		LogInfo("[%d] start doc position: %d\n", no, curPos)
	}
	var err error
	count++
	for {
		// get current position
		curPos, err = br.current()
		if err == io.EOF || end >= 0 && curPos >= end {
			LogDebug("[%d] reached end or EOF at %d\n", no, curPos)
			break
		}
		if err != nil {
			LogDebug("[%d] get current position error: %v\n", no, err)
			break
		}
		if Verbose && uint32(count) == nextVerbose {
			LogInfo("[%d] got %10d documents from %20d to position %20d\n", no, count, start, curPos)
			if verboseStep == 0 {
				nextVerbose = nextVerbose * 10
			} else {
				nextVerbose = nextVerbose + verboseStep
			}
		}
		// skip next doc
		err = br.skipNext()
		if err == io.EOF {
			LogDebug("[%d] no more doc after %d\n", no, curPos)
			break
		}
		if err != nil {
			if !skipError {
				break
			}
			LogDebug("[%d] doc read error at %d, seek for next doc\n", no, curPos)
			_, dc := br.next(curPos, -1, -1, -1, nil, keyOnly)
			if dc == nil {
				break
			}
		}
		count++
	}
	if err != nil && err != io.EOF {
		LogError("\n[%d] read doc error at %d\n%v", no, curPos, err)
		return -1
	}
	if Verbose {
		LogInfo("[%d] got %10d documents from %20d to position %20d\n", no, count, start, curPos)
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
		LogError("%v\n", err)
		return
	}
	var count int32
	doc := &DocKey{}
	count = 0
	var regex *regexp.Regexp = nil
	if opt.KeyPattern != "" {
		regex, err = regexp.Compile(opt.KeyPattern)
		if err != nil {
			LogError("regex error: %v\n", err)
			return
		}
	}

	for opt.Limit == 0 || count < opt.Limit {
		current, _ = br.current()
		_, err = br.docSeeker.ReadKey(doc)
		if err == io.EOF {
			break
		}
		if err != nil && !opt.SkipError {
			LogError("[!%d]\t%20d\t%v\n", count, current, err)
			return
		}
		if err == nil && regex != nil && !regex.MatchString(string(doc.Key)) {
			err = ErrInvalidDocument
			current += 1
		}
		if err != nil {
			pos, document := br.next(current, -1, -1, -1, regex, true)
			if document == nil {
				LogError("[!%d]\t%20d\t%v\n", count, current, err)
				return
			}
			_ = br.resetOffset(pos)
			doc.Key = CloneBytes(document.Key)
			current = pos
		}
		count++
		if keyOnly {
			fmt.Println(string(doc.Key))
		} else {
			fmt.Printf("[%d]\t%20d\t%s\n", count, current, string(doc.Key))
		}
		if opt.Step > 0 {
			_ = br.skipDocs(opt.Step)
		}
	}
	if !keyOnly {
		fmt.Printf("total %d\n", count)
	}
}

// Search document in bin file
func (br *binReader) Search(opt SearchOption) int64 {
	_, err := br.docSeeker.Seek(opt.Offset, io.SeekStart)
	if err != nil {
		LogError("%v\n", err)
		return -1
	}
	reg, err := regexp.Compile(opt.Key)
	if err != nil {
		LogError("invalid key pattern %s, %v\n", opt.Key, err)
		return -1
	}
	var docPos int64 = -1

	var found int64 = -1
	skip := opt.Skip
	if skip < 0 {
		rnd := rand.New(rand.NewSource(time.Now().Unix()))
		skip = rnd.Intn(100)
	}
	if Verbose {
		LogInfo("skip: %d\n", skip)
	}
	doc := &DocKey{}
	for {
		docPos, _ = br.docSeeker.Seek(0, io.SeekCurrent)
		_, err = br.docSeeker.ReadKey(doc)
		if err == io.EOF {
			break
		}
		if err != nil {
			pos, dc := br.next(docPos, -1, -1, -1, nil, true)
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
	startOffset := int64(-1)
	startOffset, err = br.docSeeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	offset = startOffset
	defer func() {
		if err != nil && startOffset >= 0 {
			_, _ = br.docSeeker.Seek(startOffset, io.SeekStart)
		}
	}()
	// read key size
	dk := &DocKey{}
	var n int
	n, err = br.docSeeker.ReadKey(dk)
	if err == nil {
		offset += int64(n)
	}
	return err
}

// next seek next valid doc, return doc position and doc
func (br *binReader) next(start, end int64, keySize, docSize int, regex *regexp.Regexp, decompress bool) (pos int64, doc *Doc) {

	pos = start
	if keySize <= 0 {
		keySize = int(KeySizeLimit)
	}
	err := br.resetOffset(start)
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
		LogError("read file error: %v\n", err)
		return 0, nil
	}
	var docKey *DocKey
	// number of bytes read from pos
	nBytes := int64(len(buff))
	for {
		docKey, err = br.checkKey(buff, regex, keySize, docSize)
		if err != nil {
			break
		}
		if docKey != nil {
			doc, err = br.docSeeker.ReadAt(pos, decompress)
			if doc != nil {
				break
			}
			// why not pos + 1?
			// because key is match, doc is invalid, these bytes won't be part of next valid doc
			// skip to pos + 4 + keySize
			nBytes += int64(4 + int32(docKey.KeySize))
			_, _ = br.file.Seek(pos+nBytes, io.SeekStart)
			_, err = br.file.Read(buff)
			if err != nil {
				break
			}
		} else {
			buff, err = br.readByte(buff)
			if err != nil {
				break
			}
			nBytes += 1
		}

		if Debug {
			nBytes := pos - start
			if nBytes < 1024 {
				LogInfo("%10d\t%10d bytes search\n", pos, nBytes)
			} else {
				LogInfo("%10d\t%10dk search\n", pos, nBytes/1024)
			}
		}
		if end > 0 && pos > end {
			break
		}
	}
	if err != nil {
		LogError("seek next error: %v\n", err)
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
			LogError("regex error: %v\n", err)
			return 0, nil
		}
	}
	return br.next(opt.Offset, opt.End, opt.KeySize, opt.DocSize, regex, opt.Decompress)
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
