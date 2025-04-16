package binfile

import "io"

type NoneCompressWriter struct {
	dst io.Writer
}

type NoneCompressReader struct {
	src io.Reader
}

func (w NoneCompressWriter) Write(p []byte) (int, error) {
	return w.dst.Write(p)
}

func NewNoneCompressWriter(dst io.Writer) *NoneCompressWriter {
	return &NoneCompressWriter{dst: dst}
}

func (w NoneCompressReader) Read(p []byte) (int, error) {
	return w.src.Read(p)
}

func (w NoneCompressReader) Close() error {
	if wr, ok := w.src.(io.ReadCloser); ok {
		_ = wr.Close()
	}
	return nil
}

func NewNoneCompressReader(src io.Reader) *NoneCompressReader {
	return &NoneCompressReader{src: src}
}
