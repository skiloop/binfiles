package binfile

import (
	"fmt"
	"github.com/skiloop/binfiles/workers"
	"os"
	"path"
	"regexp"
)

type pathRepack struct {
	fnCh    chan interface{}
	stopCh  chan interface{}
	src     string // source path
	dst     string // target path
	suffix  string
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
	dst := path.Join(p.dst, path.Base(filename)+"."+p.suffix)
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
	rd, err = NewBinReader(path.Join(filename), p.st)
	if err != nil {
		return err
	}
	defer rd.Close()
	br, _ := rd.(*binReader)
	var doc *Doc
	count := uint32(0)
	for {
		doc, err = br.docSeeker.Read(true)
		if err != nil {
			break
		}
		if _, err = bw.Write(doc); err != nil {
			continue
		}
		count += 1
	}
	fmt.Printf("convert %s to %s with %d docs\n", br.filename, bw.Filename(), count)
	return nil
}

func getPackageSuffix(pt int) string {
	switch pt {
	case NONE:
		return ""
	case GZIP:
		return "gz"
	case BROTLI:
		return "br"
	case BZIP2:
		return "bz2"
	case LZ4:
		return "lz4"
	case XZ:
		return "xz"
	}
	return ""
}

func (p *pathRepack) start(wc int) {
	workers.RunJobs(wc, p.stopCh, p.worker, p.seeder)
}
