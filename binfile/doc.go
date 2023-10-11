package binfile

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
)

const (
	GZIP = iota
	ZIP
	BZIP2
	BROTLI
	LZ4
)

var EmptyDocKey = "empty-doc."

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
	var keyBuf []byte
	keyBuf, err = readBytes(r, int(keySize))
	if err != nil {
		return err
	}
	doc.Key = string(keyBuf)
	valueSize, err := ReadKeySize(r)
	if err != nil {
		return err
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
		return gzip.NewReader(bytes.NewReader(doc.CompressContent))
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
		if doc.Key == EmptyDocKey {
			doc.Content = EmptyDocKey
			return &doc, nil
		}
		err = doc.Decompress()
		if err != nil {
			return nil, err
		}
	}
	return &doc, err
}
