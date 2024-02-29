package binfile

import (
	"fmt"
	"github.com/skiloop/binfiles/workers"
	"os"
	"sync"
	"sync/atomic"
)

type RepackCmd struct {
	Source              string `arg:"" help:"source bin file name"`
	Target              string `arg:"" help:"target bin file name"`
	Workers             int    `short:"w" help:"number of workers" default:"3"`
	Split               int    `help:"split target into small parts if positive every specified number of docs, 0 means not to split" default:"0"`
	TargetCompressType  string `short:"t" help:"compression type for docs in target file, none for no compression" enum:"gzip,bz2,none" default:"none"`
	PackageCompressType string `short:"c" help:"compression type after whole target completed, none for no compression" enum:"gzip,bz2,none" default:"none"`
}

type repackager struct {
	docCh   chan *Doc
	reader  *docReader
	target  string
	pt      int
	writer  *Packager
	running bool
	split   int
	idx     atomic.Int32
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
func (r *repackager) seeder(wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	fmt.Println("reader starts")
	for {
		offset, _ := r.reader.Seek(0, 1)
		doc, err := r.reader.next()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "doc read error at %d: %v\n", offset, err)
			return
		}
		if doc == nil {
			break
		}
		r.docCh <- doc
	}
	r.docCh <- nil
	fmt.Println("reader one")
}

func (r *repackager) worker(wg *sync.WaitGroup, no int) {
	fmt.Printf("worker %d started\n", no)
	wg.Add(1)
	defer wg.Done()
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
		if r.writer == nil {
			count += 1
			if count%int64(r.split) == 0 {
				writer.Close()
				writer = r.nextPackager()
				if writer == nil {
					break
				}
			}
		}
	}
	if r.writer == nil {
		writer.Close()
	}
	fmt.Printf("worker %d done\n", no)
}

func (r *repackager) start(source, target, compressionType string, workerCount int, split int) error {
	r.reader = newDocReader(source, GZIP)
	if err := r.reader.Open(); err != nil {
		return err
	}
	defer r.reader.Close()
	r.split = split
	if split > 0 {
		r.target = target
	} else {
		r.writer = newPackager(target, CompressTypes[compressionType])
		if err := r.writer.Open(); err != nil {
			return err
		}
		defer r.writer.Close()
	}

	r.pt = CompressTypes[compressionType]
	r.docCh = make(chan *Doc, workerCount+3)
	workers.RunJobs(workerCount, r.worker, r.seeder)
	return nil
}

// Repack bin file
func Repack(opt RepackCmd) error {
	r := repackager{}
	return r.start(opt.Source, opt.Target, opt.PackageCompressType, opt.Workers, opt.Split)
}
