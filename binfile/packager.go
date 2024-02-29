package binfile

import (
	"compress/gzip"
	"github.com/andybalholm/brotli"
	"github.com/dsnet/compress/bzip2"
	"github.com/pierrec/lz4"
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
	case LZ4:
		return lz4.NewWriter(fn), nil
	case BROTLI:
		return brotli.NewWriter(fn), nil
	case GZIP:
		fallthrough
	default:
		return gzip.NewWriter(fn), nil
	}
	//return nil, errors.New(fmt.Sprintf("unknown package compression type %d", compressType))
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
