package binfile

import (
	"io"
	"encoding/binary"
	"bytes"
	"errors"
	"io/ioutil"
	"compress/gzip"
	"compress/bzip2"
)

const (
	GZIP   = iota
	ZIP
	BZIP2
	BROTLI
	LZ4
)

var CompressTypes = map[string]int{
	"gzip":   GZIP,
	"zip":    ZIP,
	"bzip2":  BZIP2,
	"bz2":    BZIP2,
	"br":     BROTLI,
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

func (doc *Doc) read(r io.Reader) error {
	keySize, err := doc.read4BitsInt(r)
	if err != nil {
		return err
	}
	keyBuf := make([]byte, keySize)

	n, err := r.Read(keyBuf)
	if err != nil {
		return err
	}
	if n != keySize {
		return ErrReadKey
	}
	doc.Key = string(keyBuf)
	valueSize, err := doc.read4BitsInt(r)
	if err != nil {
		return err
	}

	valueBuf := make([]byte, valueSize)
	n, err = r.Read(valueBuf)
	if err != nil {
		return err
	}
	doc.CompressContent = valueBuf
	return nil
}

func (doc *Doc) read4BitsInt(r io.Reader) (int, error) {
	intBuf := make([]byte, 4)
	n, err := r.Read(intBuf)
	if n != 4 || err == io.EOF {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	var keySize int
	_ = binary.Read(bytes.NewBuffer(intBuf), binary.BigEndian, &keySize)
	return keySize, nil
}

type bzip2ReaderCloser struct {
	reader io.Reader
}

func (bz *bzip2ReaderCloser) Close() error {
	return nil
}

func (bz *bzip2ReaderCloser) Read(p []byte) (n int, err error) {
	return bz.reader.Read(p)
}

func (doc *Doc) getDecompressReader() (reader io.ReadCloser, err error) {
	switch doc.CompressType {
	case GZIP:
		return gzip.NewReader(bytes.NewBuffer(doc.CompressContent))
	case BZIP2:
		rc := bzip2ReaderCloser{bzip2.NewReader(bytes.NewBuffer(doc.CompressContent))}
		return &rc, nil
	default:
		return nil, ErrNotSupport
	}
}

func (doc *Doc) Decompress() error {
	reader, err := doc.getDecompressReader()
	if err != nil {
		return err
	}
	defer reader.Close()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	doc.Content = string(data)
	return nil
}

func ReadDoc(r io.Reader, compressType int, decompress bool) (*Doc, error) {
	doc := Doc{CompressType: compressType}
	err := doc.read(r)
	if err != nil {
		return nil, err
	}
	if decompress {
		err = doc.Decompress()
		if err != nil {
			return nil, err
		}
	}
	return &doc, err
}
