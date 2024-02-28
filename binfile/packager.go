package binfile

import (
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/dsnet/compress/bzip2"
	"io"
	"os"
)

type Packager struct {
	docWriter
	packageCompression int
}

func (p *Packager) Open() error {
	err := p.docWriter.Open()
	if err != nil {
		return err
	}
	p.file, err = getCompressCloser(p.packageCompression, p.fn)
	return err
}

func getCompressCloser(compressType int, fn *os.File) (io.WriteCloser, error) {
	switch compressType {
	case BZIP2:
		return bzip2.NewWriter(fn, nil)
	case GZIP:
	default:
		return gzip.NewWriter(fn), nil
	}
	return nil, errors.New(fmt.Sprintf("unknown package compression type %d", compressType))
}

func newPackager(filename string, packageCompressType int) *Packager {
	return &Packager{
		docWriter: docWriter{
			binWriterFile: binWriterFile{
				binFile: binFile{filename: filename, compressType: NONE},
			},
		},
		packageCompression: packageCompressType,
	}
}
