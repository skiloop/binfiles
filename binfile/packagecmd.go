package binfile

import (
	"errors"
	"fmt"
	"github.com/skiloop/binfiles/workers"
	"io"
	"os"
	"sync/atomic"
)

const end = "END"

type RepackCmd struct {
	Source              string `arg:"" help:"source bin file name"`
	Target              string `arg:"" help:"target bin file name"`
	Workers             int    `short:"w" help:"number of workers" default:"3"`
	Merge               bool   `short:"m" help:"merge result into one when split > 0 or workers > 1"`
	Split               int    `help:"max number of package, no limit if 0" default:"0"`
	TargetCompressType  string `short:"t" help:"target doc compression type" enum:"gzip,bzip2,bz2,br,brotli,none" default:"none"`
	PackageCompressType string `short:"c" help:"package compression type" enum:"gzip,bz2,bzip2,xz,lz4,br,brotli,none" default:"none"`
}

type repackager struct {
	docCh        chan *Doc
	filenameCh   chan string
	workerStopCh chan interface{}
	reader       *docReader
	target       string
	pt           int
	writer       *Packager
	running      bool
	merge        bool
	split        int
	idx          atomic.Int32
}

func (r *repackager) nextPackager() *Packager {
	no := r.idx.Add(1)
	filename := fmt.Sprintf("%s.%d", r.target, no)
	writer := newPackager(filename, r.pt)
	if err := writer.Open(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to open %s\n", filename)
		return nil
	}
	return writer
}

func (r *repackager) seeder() {
	fmt.Println("reader starts")
	for {
		offset, _ := r.reader.Seek(0, 1)
		doc, err := r.reader.next()
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
		r.docCh <- doc
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
		if filename == end {
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
	if r.merge {
		r.filenameCh <- filename
	}
}

func (r *repackager) worker(no int) {
	fmt.Printf("worker %d started\n", no)
	var err error
	var writer *Packager
	if r.writer != nil {
		writer = r.writer
	} else {
		writer = r.nextPackager()
		if writer == nil {
			return
		}
	}
	var count int64
	count = 0
	for {
		doc := <-r.docCh
		if doc == nil {
			r.docCh <- doc
			r.notifyMerge(writer.filename)
			break
		}
		if Verbose {
			fmt.Printf("[%d]package %s\n", no, doc.Key)
		}
		err = writer.Write(doc)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[%d]write error: %s, %v\n", no, doc.Key, err)
			continue
		}
		if r.writer == nil && r.split > 0 {
			count += 1
			if count%int64(r.split) == 0 {
				writer.Close()
				r.notifyMerge(writer.filename)
				writer = r.nextPackager()
				if writer == nil {
					_, _ = fmt.Fprintf(os.Stderr, "[%d]failed to get next packager\n", no)
					break
				}
			}
		}
	}
	if r.writer == nil {
		writer.Close()
	}
	r.workerStopCh <- nil
	fmt.Printf("worker %d done\n", no)
}

func (r *repackager) start(source string, workerCount int) error {
	r.reader = newDocReader(source, GZIP)
	if err := r.reader.Open(); err != nil {
		return err
	}
	defer r.reader.Close()
	var stopCh chan interface{}
	if r.split <= 0 && workerCount == 1 {
		r.writer = newPackager(r.target, r.pt)
		if err := r.writer.Open(); err != nil {
			return err
		}
		defer r.writer.Close()
	}
	if r.merge {
		stopCh = make(chan interface{})
		r.filenameCh = make(chan string, workerCount)
		go r.merger(stopCh)
	}
	workers.RunJobs(workerCount, r.workerStopCh, false, r.worker, r.seeder)
	if r.merge {
		r.filenameCh <- end
		<-stopCh
	}
	return nil
}

// Repack bin file
func Repack(opt RepackCmd) error {
	r := repackager{
		docCh:        make(chan *Doc, opt.Workers+3),
		filenameCh:   nil,
		workerStopCh: make(chan interface{}),
		reader:       nil,
		target:       opt.Target,
		pt:           CompressTypes[opt.PackageCompressType],
		writer:       nil,
		running:      false,
		merge:        opt.Merge && (opt.Split > 1 || opt.Workers > 1),
		split:        opt.Split,
		idx:          atomic.Int32{},
	}
	return r.start(opt.Source, opt.Workers)
}
