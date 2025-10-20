package binfile

import (
	"archive/tar"
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ulikunitz/xz"
)

// ErrStopTarWalk can be returned by the handler to stop walking without error.
var ErrStopTarWalk = errors.New("stop tar walk")

// WalkTar streams a TAR archive from reader and invokes handler for each entry.
// The handler receives the current entry header and an io.Reader positioned at
// the entry's content. The handler should read exactly the bytes it needs; any
// unread bytes will be discarded automatically as the next entry is sought.
//
// Return ErrStopTarWalk from handler to stop iteration early without error.
func WalkTar(reader io.Reader, handler func(h *tar.Header, r io.Reader) error) error {
	tr := tar.NewReader(reader)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := handler(hdr, tr); err != nil {
			if errors.Is(err, ErrStopTarWalk) {
				return nil
			}
			return err
		}
	}
}

// WalkTarXZ is like WalkTar but expects the input to be xz-compressed TAR.
// It streams decompression and does not require temporary files.
func WalkTarXZ(reader io.Reader, handler func(h *tar.Header, r io.Reader) error) error {
	xzr, err := xz.NewReader(reader)
	if err != nil {
		return err
	}
	return WalkTar(xzr, handler)
}

// CompressionFormat indicates the compression format of the TAR stream.
// Supported values: "auto", "none", "gzip", "xz", "bzip2", "zlib".
type CompressionFormat string

const (
	CompressionAuto  CompressionFormat = "auto"
	CompressionNone  CompressionFormat = "none"
	CompressionGzip  CompressionFormat = "gzip"
	CompressionXZ    CompressionFormat = "xz"
	CompressionBzip2 CompressionFormat = "bzip2"
	CompressionZlib  CompressionFormat = "zlib"
)

// WalkTarCompressed streams a possibly-compressed TAR. If format is "auto" or empty,
// it detects the compression by magic header. Readers that require Close will be closed
// automatically when processing completes.
func WalkTarCompressed(reader io.Reader, format CompressionFormat, handler func(h *tar.Header, r io.Reader) error) error {
	if format == "" {
		format = CompressionAuto
	}
	br := bufio.NewReader(reader)
	if format == CompressionAuto {
		format = detectCompression(br)
	}

	var (
		decompressed io.Reader
		closer       io.Closer
		err          error
	)

	switch strings.ToLower(string(format)) {
	case string(CompressionNone):
		decompressed = br
	case string(CompressionGzip):
		var gz *gzip.Reader
		gz, err = gzip.NewReader(br)
		if err != nil {
			return err
		}
		decompressed = gz
		closer = gz
	case string(CompressionXZ):
		decompressed, err = xz.NewReader(br)
		if err != nil {
			return err
		}
	case string(CompressionBzip2):
		decompressed = bzip2.NewReader(br)
	case string(CompressionZlib):
		var zr io.ReadCloser
		zr, err = zlib.NewReader(br)
		if err != nil {
			return err
		}
		decompressed = zr
		closer = zr
	default:
		// Fallback to none if unknown
		decompressed = br
	}

	if closer != nil {
		defer func() { _ = closer.Close() }()
	}
	return WalkTar(decompressed, handler)
}

// WalkTarAuto is a helper that auto-detects compression and walks the TAR.
func WalkTarAuto(reader io.Reader, handler func(h *tar.Header, r io.Reader) error) error {
	return WalkTarCompressed(reader, CompressionAuto, handler)
}

// detectCompression inspects initial bytes to guess compression format.
func detectCompression(br *bufio.Reader) CompressionFormat {
	// Peek up to 6 bytes which covers most magic numbers we need.
	magic, _ := br.Peek(6)
	if len(magic) >= 2 {
		// gzip: 1F 8B
		if magic[0] == 0x1F && magic[1] == 0x8B {
			return CompressionGzip
		}
	}
	if len(magic) >= 6 {
		// xz: FD 37 7A 58 5A 00
		if magic[0] == 0xFD && magic[1] == 0x37 && magic[2] == 0x7A && magic[3] == 0x58 && magic[4] == 0x5A && magic[5] == 0x00 {
			return CompressionXZ
		}
	}
	if len(magic) >= 4 {
		// zlib: 78 01/9C/DA (we just check 0x78 as heuristic)
		if magic[0] == 0x78 {
			return CompressionZlib
		}
	}
	if len(magic) >= 3 {
		// bzip2: 42 5A 68 ("BZh")
		if magic[0] == 'B' && magic[1] == 'Z' && magic[2] == 'h' {
			return CompressionBzip2
		}
	}
	return CompressionNone
}

// ListTar lists the headers of the TAR archive.
func ListTar(filename string, format CompressionFormat, limit int) {
	file, err := os.Open(filename)
	if err != nil {
		LogError("failed to open %s: %v\n", filename, err)

	}
	defer file.Close()
	WalkTarCompressed(file, format, func(h *tar.Header, r io.Reader) error {
		fmt.Printf("%s %d\n", h.Name, h.Size)
		if limit > 0 {
			limit--
			if limit <= 0 {
				return ErrStopTarWalk
			}
		}
		return nil
	})
}
