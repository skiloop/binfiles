package binfile

import (
	"fmt"
	"os"
)

type Repackager struct {
	filename           string
	writer             DocWriter
	packageCompression int
	file               *os.File
}

func (r *Repackager) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

func newRepackager(filename string, packageCompressType int) *Repackager {
	file, err := os.OpenFile(filename, writerFileFlag, 0644)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error opening %s: %v\n", filename, err)
		return nil
	}
	compressor, err := getCompressCloser(packageCompressType, file)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		_ = file.Close()
		return nil
	}
	return &Repackager{
		filename:           filename,
		writer:             NewDocWriter(compressor),
		packageCompression: packageCompressType,
		file:               file,
	}
}
