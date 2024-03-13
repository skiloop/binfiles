package binfile

import (
	"fmt"
	"github.com/skiloop/binfiles/workers"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
)

type pathRepack struct {
	fnCh    chan interface{}
	stopCh  chan interface{}
	src     string         // source path
	dst     string         // target path
	pattern *regexp.Regexp // file pattern
	pt      int
	tt      int
	st      int
}

func (p *pathRepack) seeder() {
	searchFiles(p.src, p.fnCh, p.stopCh, p.pattern)
}

func (p *pathRepack) worker(no int) {
	fmt.Printf("[%d] worker starts\n", no)
	count := 0
	for {
		fn := <-p.fnCh
		if fn == nil {
			p.fnCh <- fn
			break
		}
		if filename, ok := fn.(string); ok {
			fmt.Printf("packaging %s\n", filename)
			if err := p.pack(filename); err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "bin file error: %s\n", filename)
				continue
			}
			count += 1
		}
	}
	fmt.Printf("[%d] worker done with %d files\n", no, count)
}
func (p *pathRepack) pack(filename string) (err error) {
	dst := filepath.Join(p.dst, path.Base(filename)+getPackageSuffix(p.pt))
	if CheckFileExists(dst) {
		_, _ = fmt.Fprintf(os.Stderr, "file already exists: %s\n", dst)
		return ErrFileExists
	}
	bw := NewCCBinWriter(dst, p.pt, p.tt)

	if err = bw.Open(); err != nil {
		return err
	}
	defer bw.Close()
	var rd BinReader
	rd, err = NewBinReader(filepath.Join(filename), p.st)
	if err != nil {
		return err
	}
	defer rd.Close()
	br, _ := rd.(*binReader)
	var doc *Doc
	count := uint32(0)
	skip := uint32(0)
	running := true
	for running {
		doc, err = br.docSeeker.Read(true)
		if err != nil && err != io.EOF {
			offset, _ := br.docSeeker.Seek(0, io.SeekCurrent)
			_, _ = fmt.Fprintf(os.Stderr, "error at offset %d:  %v\n", offset, err)
			break
		}
		running = err != io.EOF
		if doc == nil {
			continue
		}
		if _, err = bw.Write(doc); err != nil {
			debug("write doc %s error: %v\n", doc.Key, err)
			skip += 1
			continue
		}
		count += 1
	}
	fmt.Printf("convert %s to %s with %d docs and skip %d docs\n", br.filename, bw.Filename(), count, skip)
	return nil
}

func getPackageSuffix(pt int) string {
	switch pt {
	case NONE:
		return ""
	case GZIP:
		return ".gz"
	case BROTLI:
		return ".br"
	case BZIP2:
		return ".bz2"
	case LZ4:
		return ".lz4"
	case XZ:
		return ".xz"
	}
	return ""
}

func (p *pathRepack) start(wc int) {
	workers.RunJobs(wc, p.stopCh, p.worker, p.seeder)
}
