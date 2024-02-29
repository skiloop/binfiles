package binfile

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
