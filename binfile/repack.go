package binfile

import (
	"fmt"
	"os"
	"regexp"
	"sync/atomic"
)

type RepackCmd struct {
	Source              string `arg:"" help:"source bin file name or directory"`
	Target              string `arg:"" help:"target bin file name or directory"`
	Workers             int    `short:"w" help:"number of workers" default:"3"`
	Split               int    `help:"max number of package, no limit if 0" default:"0"`
	Limit               int    `help:"max number of docs, no limit if 0" default:"0"`
	Pattern             string `short:"p" help:"file name pattern for path mode"`
	ContentOnly         bool   `short:"o" help:"write content only, target compress type will have no effect" default:"false"`
	Mode                string `short:"m" help:"mode of repack, doc for worker on reading doc, file for on packaging, path for multiple files and source and target are directory" enum:"file,doc,path" default:"file"`
	SourceCompressType  string `short:"i" help:"source bin compression type" enum:"gzip,bzip2,bz2,br,brotli,none" default:"gzip"`
	TargetCompressType  string `short:"t" help:"target bin compression type" enum:"gzip,bzip2,bz2,br,brotli,none" default:"none"`
	PackageCompressType string `short:"c" help:"package compression type" enum:"gzip,bz2,bzip2,xz,lz4,br,brotli,none" default:"none"`
}

const workerEndFlag = ""

// Repack bin file
func Repack(opt RepackCmd) error {
	// repack with multiple workers to read docs from different files
	if opt.Mode == "file" {
		r := fileRepack{
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
		// no decompress and compression when input and outWriter are the same
		if r.st == r.tt {
			r.tt = NONE
			r.st = NONE
		}
		return r.start(opt.Source, opt.Workers)
	}
	// repack using multiple workers on reading docs from the same file
	if opt.Mode == "doc" {
		r := docRepack{
			limit:       opt.Limit,
			contentOnly: opt.ContentOnly,
			source:      opt.Source,
			target:      opt.Target,
			pt:          CompressTypes[opt.PackageCompressType],
			tt:          CompressTypes[opt.TargetCompressType],
			st:          CompressTypes[opt.SourceCompressType],
			split:       opt.Split,
			pos:         atomic.Int64{},
		}
		// no decompress and compression when input and outWriter are the same
		if r.st == r.tt {
			r.tt = NONE
			r.st = NONE
		}
		return r.start(opt.Workers)
	}

	r := pathRepack{
		fnCh:    make(chan interface{}, opt.Workers),
		stopCh:  make(chan interface{}),
		src:     opt.Source,
		dst:     opt.Target,
		pattern: nil,
		pt:      CompressTypes[opt.PackageCompressType],
		tt:      CompressTypes[opt.TargetCompressType],
		st:      CompressTypes[opt.SourceCompressType],
	}
	fmt.Printf("file pattern: %s\n", opt.Pattern)
	if opt.Pattern != "" {
		p, err := regexp.Compile(opt.Pattern)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "regex error: %v\n", err)
			return err
		}
		r.pattern = p
	}
	r.start(opt.Workers)
	return nil
}
