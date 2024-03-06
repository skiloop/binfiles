package binfile

import (
	"errors"
	"fmt"
	"github.com/skiloop/binfiles/workers"
	"io"
	"os"
	"sync/atomic"
)

type RepackCmd struct {
	Source              string `arg:"" help:"source bin file name"`
	Target              string `arg:"" help:"target bin file name"`
	Workers             int    `short:"w" help:"number of workers" default:"3"`
	Split               int    `help:"max number of package, no limit if 0" default:"0"`
	Limit               int    `help:"max number of docs, no limit if 0" default:"0"`
	SourceCompressType  string `short:"i" help:"source bin compression type" enum:"gzip,bzip2,bz2,br,brotli,none" default:"gzip"`
	TargetCompressType  string `short:"t" help:"target bin compression type" enum:"gzip,bzip2,bz2,br,brotli,none" default:"none"`
	PackageCompressType string `short:"c" help:"package compression type" enum:"gzip,bz2,bzip2,xz,lz4,br,brotli,none" default:"none"`
}

type repackager struct {
	docCh      chan *Doc
	filenameCh chan string
	stopSeeder chan interface{}
	reader     SeekReader
	limit      int
	target     string
	pt         int
	tt         int
	st         int
	split      int
	idx        atomic.Int32
}

func (r *repackager) nextBinWriter() BinWriter {
	no := r.idx.Add(1)
	filename := fmt.Sprintf("%s.%d", r.target, no)
	return NewBinWriter(filename, r.tt)
}

func (r *repackager) seeder() {
	fmt.Println("reader starts")
	count := 0
	for {
		offset, _ := r.reader.Seek(0, io.SeekCurrent)
		doc, err := r.reader.Read(true)
		if err == io.EOF {
			break
		}
		if errors.Is(err, ErrValueDecompress) {
			_, _ = fmt.Fprintf(os.Stderr, "doc read error at %d: %v\n", offset, err)
			continue
		}
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "doc read error at %d: %v\n", offset, err)
			break
		}
		if doc == nil {
			break
		}
		select {
		case r.docCh <- doc:
		case <-r.stopSeeder:
			break
		}
		count += 1
		if r.limit > 0 && count >= r.limit {
			break
		}
	}
	r.docCh <- nil
	fmt.Println("reader done")
}
func closeWriter(closer io.Closer, msg string) {
	err := closer.Close()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s close error: %v\n", msg, err)
	}
}
func (r *repackager) merge(stopCh chan interface{}) {

	wtr, err := os.OpenFile(r.target, writerFileFlag, 0644)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "fail to open file %s: %v\n", r.target, err)
		return
	}
	defer closeWriter(wtr, r.target)

	wc, err := getCompressWriter(r.pt, wtr)
	if err != nil {
		return
	}
	defer closeWriter(wc, "compressor")
	defer func() {
		if err := recover(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "merger recover: %v\n", err)
		}
	}()
	var rdr *os.File
	failed := false
	var nw int64
	for {
		filename := <-r.filenameCh
		if filename == workerEndFlag {
			break
		}
		if failed {
			continue
		}
		rdr, err = os.OpenFile(filename, os.O_RDONLY, 0644)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "fail to open file %s: %v\n", filename, err)
			failed = true
			continue
		}
		fmt.Printf("merging %s\n", filename)
		nw, err = io.Copy(wc, rdr)
		_ = rdr.Close()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "fail to append file %s: %v\n", filename, err)
			failed = true
			continue
		}
		//_ = os.Remove(filename)
		if Debug {
			fmt.Printf("%s merged with %d bytes\n", filename, nw)
		}
	}
	stopCh <- nil
	fmt.Println("merger done")
}

func (r *repackager) worker(no int) {
	fmt.Printf("worker %d started\n", no)
	var err error
	rp := r.nextBinWriter()
	err = rp.Open()
	if err != nil {
		return
	}

	var count int64 = 0
	for {
		doc := <-r.docCh
		if doc == nil {
			r.docCh <- doc
			r.filenameCh <- rp.Filename()
			break
		}
		if Verbose {
			fmt.Printf("[%d]package %s\n", no, doc.Key)
		}
		_, err = rp.Write(doc)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[%d]write error: %s, %v\n", no, doc.Key, err)
			continue
		}
		if r.split > 0 {
			count += 1
			if count%int64(r.split) == 0 {
				rp.Close()
				r.filenameCh <- rp.Filename()
				rp = r.nextBinWriter()
				err = rp.Open()
				if err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "[%d]failed to get next packager: %v\n", no, err)
					break
				}
			}
		}
	}
	rp.Close()
	fmt.Printf("worker %d done\n", no)
}

func (r *repackager) start(source string, workerCount int) error {
	fn, err := os.OpenFile(source, os.O_RDONLY, 0644)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to open %s: %v\n", source, err)
		return err
	}
	r.reader = NewSeeker(fn, r.st)
	defer func(r io.Closer) {
		_ = r.Close()
	}(r.reader)

	waitMergerCh := make(chan interface{})
	go r.merge(waitMergerCh)

	workers.RunJobs(workerCount, r.stopSeeder, r.worker, r.seeder)

	r.filenameCh <- workerEndFlag
	<-waitMergerCh
	return nil
}

// Repack bin file
func Repack(opt RepackCmd) error {
	r := repackager{
		docCh:      make(chan *Doc, opt.Workers+3),
		filenameCh: make(chan string, opt.Workers),
		stopSeeder: make(chan interface{}),
		reader:     nil,
		limit:      opt.Limit,
		target:     opt.Target,
		pt:         CompressTypes[opt.PackageCompressType],
		tt:         CompressTypes[opt.TargetCompressType],
		st:         CompressTypes[opt.SourceCompressType],
		split:      opt.Split,
		idx:        atomic.Int32{},
	}
	// no decompress and compression when input and output are the same
	if r.st == r.tt {
		r.tt = NONE
		r.st = NONE
	}
	return r.start(opt.Source, opt.Workers)
}
