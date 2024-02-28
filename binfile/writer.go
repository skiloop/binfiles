package binfile

import "sync"

type PackageOption struct {
	Path          string `doc:"source path"`
	Pattern       string `doc:"file pattern,those match will be packaged. all files include if empty"`
	InputCompress int    `doc:"source file compression type package"`
	WorkerCount   int    `doc:"worker count"`
}

type BinWriter interface {
	// Close writer
	Close()

	// Package files to bin file
	Package(option *PackageOption) error
}

func NewBinWriter(filename string, compressType int) BinWriter {
	bf := newBinWriterFile(filename, compressType)
	if bf == nil {
		return nil
	}
	return &docWriter{binWriterFile: *bf, mu: sync.Mutex{}}
}
