package binfile

import (
	"bufio"
	"errors"
	"io"
)

// bufBinWriter is a buffered binWriter that uses bufio.Writer to reduce system calls
type bufBinWriter struct {
	*binWriter
	bufWriter  *bufio.Writer
	bufSize    int       // buffer size, default 64KB
	underlying io.Writer // underlying writer (usually *os.File)
}

// NewBufBinWriter creates a buffered BinWriter
// bufSize: buffer size, 0 means use default 64KB
func NewBufBinWriter(filename string, compressType int, bufSize int) BinWriter {
	if bufSize <= 0 {
		bufSize = 64 * 1024 // default 64KB
	}
	return &bufBinWriter{
		binWriter: createBinWriter(filename, compressType),
		bufSize:   bufSize,
	}
}

// Open opens the file and creates a buffered writer
func (dw *bufBinWriter) Open() error {
	if dw.file != nil {
		return nil
	}

	// Call parent's Open method to open the file
	err := dw.binWriter.Open()
	if err != nil {
		return err
	}

	// Save underlying writer (usually *os.File)
	dw.underlying = dw.writer

	// Create bufio.Writer wrapping the underlying writer
	dw.bufWriter = bufio.NewWriterSize(dw.underlying, dw.bufSize)

	// Point writer to buffered writer so all writes go through buffer
	dw.writer = dw.bufWriter

	return nil
}

// Close closes the file and flushes the buffer
func (dw *bufBinWriter) Close() error {
	if dw.file == nil {
		return nil
	}

	// Flush buffer first to ensure all data is written to file
	if dw.bufWriter != nil {
		if err := dw.bufWriter.Flush(); err != nil {
			LogError("flush buffer error: %v\n", err)
			// Continue with close operation even if flush fails
		}
		dw.bufWriter = nil
	}

	// Restore original writer (for completeness, though won't be used after Close)
	if dw.underlying != nil {
		dw.writer = dw.underlying
		dw.underlying = nil
	}

	// Call parent's Close method to close the file
	err := dw.binWriter.Close()
	if err != nil {
		return err
	}

	return nil
}

// Write writes a document (uses parent method, but goes through buffer)
func (dw *bufBinWriter) Write(doc *Doc) (int, error) {
	if dw.file == nil {
		return 0, errors.New("not opened yet")
	}
	// Use parent's Write method directly since writer already points to bufWriter
	return dw.binWriter.Write(doc)
}

// Filename returns the filename (uses parent method)
func (dw *bufBinWriter) Filename() string {
	return dw.binWriter.Filename()
}

// Flush manually flushes the buffer (optional method for scenarios requiring immediate write)
func (dw *bufBinWriter) Flush() error {
	if dw.bufWriter != nil {
		return dw.bufWriter.Flush()
	}
	return nil
}
