package binfile

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dsnet/compress/bzip2"
	"io"
	"os"
)

const (
	GZIP = iota
	NONE // do not compress
	ZIP
	BZIP2
	BROTLI
	LZ4
	XZ
)

var KeySizeLimit int32 = 1000
var EmptyDocKey = "empty-doc."

var ValueDecompressError = errors.New("value decompress error")

var CompressTypes = map[string]int{
	"gzip":   GZIP,
	"none":   NONE,
	"zip":    ZIP,
	"bzip2":  BZIP2,
	"bz2":    BZIP2,
	"br":     BROTLI,
	"xz":     XZ,
	"brotli": BROTLI,
	"lz4":    LZ4,
}

var (
	ErrReadKey    = errors.New("key read error")
	ErrNotSupport = errors.New("not support for this compression type")
)

type Doc struct {
	Key             string
	Content         string
	CompressContent []byte
	CompressType    int
}

type DocKey struct {
	KeySize     int32
	ContentSize int32
	Key         string
}

func (doc *Doc) read(r io.Reader) error {
	keySize, err := ReadKeySize(r)
	if err != nil {
		return err
	}
	if keySize <= 0 || keySize > KeySizeLimit {
		return InvalidDocumentFound
	}
	var keyBuf []byte
	if Debug {
		fmt.Printf("read %d bytes for key\n", keySize)
	}
	keyBuf, err = readBytes(r, int(keySize))
	if err != nil {
		return err
	}
	doc.Key = string(keyBuf)
	valueSize, err := ReadKeySize(r)
	if err != nil {
		return err
	}
	if valueSize < 0 {
		return nil
	}

	valueBuf := make([]byte, valueSize)
	_, err = r.Read(valueBuf)
	if err != nil {
		return err
	}
	doc.CompressContent = valueBuf
	return nil
}

func readBytes(r io.Reader, size int) ([]byte, error) {
	keyBuf := make([]byte, size)
	n, err := r.Read(keyBuf)
	if err != nil {
		return nil, err
	}
	if n != size {
		return nil, ErrReadKey
	}
	return keyBuf, nil
}

func ReadKeySize(r io.Reader) (int32, error) {
	var keySize int32
	err := binary.Read(r, binary.LittleEndian, &keySize)
	if err != nil {
		return 0, err
	}
	return keySize, nil
}

//
//type bzip2Reader struct {
//	reader io.Reader
//}
//
//func (bz *bzip2Reader) Close() error {
//	return nil
//}
//
//func (bz *bzip2Reader) Read(p []byte) (n int, err error) {
//	return bz.reader.Read(p)
//}

func (doc *Doc) getDecompressReader() (reader io.ReadCloser, err error) {
	switch doc.CompressType {
	case GZIP:
		return gzip.NewReader(bytes.NewReader(doc.CompressContent))
	case BZIP2:
		return bzip2.NewReader(bytes.NewBuffer(doc.CompressContent), nil)
	default:
		return nil, ErrNotSupport
	}
}

func (doc *Doc) Decompress() error {
	if doc.CompressType != NONE {
		reader, err := doc.getDecompressReader()
		if err != nil {
			return err
		}
		defer func(reader io.ReadCloser) {
			_ = reader.Close()
		}(reader)
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		doc.Content = string(data)
	} else {
		doc.Content = string(doc.CompressContent)
	}

	return nil
}

func ReadDoc(r io.Reader, compressType int, decompress bool) (*Doc, error) {
	doc := Doc{CompressType: compressType}
	err := doc.read(r)
	if err != nil {
		return nil, err
	}
	if decompress {
		if doc.Key == EmptyDocKey {
			doc.Content = EmptyDocKey
			return &doc, nil
		}
		err = doc.Decompress()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "decompress error: %v\n", err)
			return nil, ValueDecompressError
		}
	}
	return &doc, err
}

// writeDoc Write document to writer
func (doc *Doc) writeDoc(w io.Writer) error {
	var data []byte
	var err error
	switch doc.CompressType {
	case NONE:
		data = []byte(doc.Content)
	case BZIP2:
		buf := bytes.Buffer{}
		writer, _ := bzip2.NewWriter(&buf, nil)
		_, _ = writer.Write([]byte(doc.Content))
		data = buf.Bytes()
	case GZIP:
		buf := bytes.Buffer{}
		writer := gzip.NewWriter(&buf)
		_, _ = writer.Write([]byte(doc.Content))
		data = buf.Bytes()
	default:
		err = ErrNotSupport
	}
	if err != nil {
		return err
	}

	key := []byte(doc.Key)
	err = writeNode(w, key)
	if err != nil {
		return err
	}
	return writeNode(w, data)
}

func writeNode(w io.Writer, data []byte) (err error) {
	keySize := int32(len(data))
	err = binary.Write(w, binary.LittleEndian, &keySize)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}
