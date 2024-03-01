package binfile

type Repackager struct {
	docWriter
	packageCompression int
}

func (p *Repackager) Open() error {
	err := p.docWriter.Open()
	if err != nil {
		return err
	}
	p.file, err = getCompressCloser(p.packageCompression, p.fn)
	return err
}

func newRepackager(filename string, packageCompressType int) *Repackager {
	return &Repackager{
		docWriter: docWriter{
			binWriterFile: binWriterFile{
				binFile: binFile{filename: filename, compressType: NONE},
			},
		},
		packageCompression: packageCompressType,
	}
}
