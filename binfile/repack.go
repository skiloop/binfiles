package binfile

import (
	"sync/atomic"
)

type RepackCmd struct {
	Source              string `arg:"" help:"source bin file name"`
	Target              string `arg:"" help:"target bin file name"`
	Workers             int    `short:"w" help:"number of workers" default:"3"`
	Split               int    `help:"max number of package, no limit if 0" default:"0"`
	Limit               int    `help:"max number of docs, no limit if 0" default:"0"`
	Mode                string `short:"m" help:"mode of repack, doc for worker on reading doc, file for on packaging" enum:"file,doc" default:"file"`
	SourceCompressType  string `short:"i" help:"source bin compression type" enum:"gzip,bzip2,bz2,br,brotli,none" default:"gzip"`
	TargetCompressType  string `short:"t" help:"target bin compression type" enum:"gzip,bzip2,bz2,br,brotli,none" default:"none"`
	PackageCompressType string `short:"c" help:"package compression type" enum:"gzip,bz2,bzip2,xz,lz4,br,brotli,none" default:"none"`
}

const workerEndFlag = ""

// Repack bin file
func Repack(opt RepackCmd) error {
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
		// no decompress and compression when input and output are the same
		if r.st == r.tt {
			r.tt = NONE
			r.st = NONE
		}
		return r.start(opt.Source, opt.Workers)
	}
	r := docRepack{
		docCh:  make(chan *Doc, opt.Workers+3),
		stopCh: make(chan interface{}),
		limit:  opt.Limit,
		source: opt.Source,
		target: opt.Target,
		pt:     CompressTypes[opt.PackageCompressType],
		tt:     CompressTypes[opt.TargetCompressType],
		st:     CompressTypes[opt.SourceCompressType],
		split:  opt.Split,
		pos:    atomic.Int64{},
	}
	// no decompress and compression when input and output are the same
	if r.st == r.tt {
		r.tt = NONE
		r.st = NONE
	}
	return r.start(opt.Workers)
}
