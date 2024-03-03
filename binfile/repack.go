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
	Merge               bool   `short:"m" help:"merge result into one when split > 0 or workers > 1"`
	Split               int    `help:"max number of package, no limit if 0" default:"0"`
	Limit               int    `help:"max number of docs, no limit if 0" default:"0"`
	TargetCompressType  string `short:"t" help:"target doc compression type" enum:"gzip,bzip2,bz2,br,brotli,none" default:"none"`
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
	writer     *Repackager
	running    bool
	split      int
	idx        atomic.Int32
}

func (r *repackager) nextPackager() *Repackager {
	no := r.idx.Add(1)
	filename := fmt.Sprintf("%s.%d", r.target, no)
	return newRepackager(filename, r.pt)
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

func (r *repackager) merger(stopCh chan interface{}) {
	wtr, err := os.OpenFile(r.target, writerFileFlag, 0644)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "fail to open file %s: %v\n", r.target, err)
		return
	}
	defer func(wtr *os.File) {
		_ = wtr.Close()
	}(wtr)
	var rdr *os.File
	failed := false
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
		_, err = io.Copy(wtr, rdr)
		_ = rdr.Close()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "fail to append file %s: %v\n", filename, err)
			failed = true
			continue
		}
		_ = os.Remove(filename)
	}
	stopCh <- nil
	fmt.Println("merger done")
}

func (r *repackager) notifyMerge(filename string) {
	if r.filenameCh != nil {
		r.filenameCh <- filename
	}
}

func (r *repackager) worker(no int) {
	fmt.Printf("worker %d started\n", no)
	var err error
	var rp *Repackager
	if r.writer != nil {
		rp = r.writer
	} else {
		rp = r.nextPackager()
		if rp == nil {
			return
		}
	}
	var count int64
	count = 0
	for {
		doc := <-r.docCh
		if doc == nil {
			r.docCh <- doc
			r.notifyMerge(rp.filename)
			break
		}
		if Verbose {
			fmt.Printf("[%d]package %s\n", no, doc.Key)
		}
		_, err = rp.writer.Write(doc)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[%d]write error: %s, %v\n", no, doc.Key, err)
			continue
		}
		if r.writer == nil && r.split > 0 {
			count += 1
			if count%int64(r.split) == 0 {
				_ = rp.Close()
				r.notifyMerge(rp.filename)
				rp = r.nextPackager()
				if rp == nil {
					_, _ = fmt.Fprintf(os.Stderr, "[%d]failed to get next packager\n", no)
					break
				}
			}
		}
	}
	if r.writer == nil {
		rp.Close()
	}
	fmt.Printf("worker %d done\n", no)
}

func (r *repackager) start(source string, workerCount int) error {
	fn, err := os.OpenFile(source, os.O_RDONLY, 0644)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to open %s: %v\n", source, err)
		return err
	}
	r.reader = NewSeeker(fn, GZIP)
	defer func(reader io.Closer) {
		_ = reader.Close()
	}(r.reader)
	var waitMergerCh chan interface{} = nil
	if r.split <= 0 && workerCount == 1 {
		r.writer = newRepackager(r.target, r.pt)

		defer func(writer *Repackager) {
			_ = writer.Close()
		}(r.writer)
	}
	if r.filenameCh != nil {
		waitMergerCh = make(chan interface{})
		go r.merger(waitMergerCh)
	}
	workers.RunJobs(workerCount, r.stopSeeder, r.worker, r.seeder)
	if r.filenameCh != nil {
		r.filenameCh <- workerEndFlag
		<-waitMergerCh
	}
	return nil
}

// Repack bin file
func Repack(opt RepackCmd) error {
	r := repackager{
		docCh:      make(chan *Doc, opt.Workers+3),
		filenameCh: nil,
		stopSeeder: make(chan interface{}),
		reader:     nil,
		limit:      opt.Limit,
		target:     opt.Target,
		pt:         CompressTypes[opt.PackageCompressType],
		writer:     nil,
		running:    false,
		split:      opt.Split,
		idx:        atomic.Int32{},
	}
	if opt.Merge && (opt.Split > 1 || opt.Workers > 1) {
		r.filenameCh = make(chan string, opt.Workers)
	}
	return r.start(opt.Source, opt.Workers)
}
