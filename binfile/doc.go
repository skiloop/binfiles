package binfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

var (
	ErrValueDecompress = errors.New("value decompress error")
	ErrReadKey         = errors.New("key read error")
	//ErrNotSupport      = errors.New("not support for this compression type")
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

func (doc *Doc) Decompress() error {
	if NONE == doc.CompressType {
		doc.Content = string(doc.CompressContent)
	} else {

		reader, err := getDecompressReader(doc.CompressType, bytes.NewReader(doc.CompressContent))
		if err != nil {
			return err
		}
		var data []byte
		data, err = io.ReadAll(reader)
		if err != nil {
			return err
		}
		doc.Content = string(data)
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
			return nil, ErrValueDecompress
		}
	}
	return &doc, err
}

// writeDoc Write document to writer
func (doc *Doc) writeDoc(w io.Writer) error {
	var data []byte
	var err error
	if doc.CompressType == NONE {
		data = []byte(doc.Content)
	} else {
		buf := bytes.Buffer{}
		writer, _ := getCompressCloser(doc.CompressType, &buf)
		_, err = writer.Write([]byte(doc.Content))
		if err != nil {
			return err
		}
		_ = writer.Close()
		//err = flush(writer)
		//if err != nil {
		//	return err
		//}
		data = buf.Bytes()
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
