package binfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"
)

var (
	ErrDecompressReader = errors.New("fail to get decompress reader")
	ErrCompressWriter   = errors.New("fail to get compress writer")
	ErrValueDecompress  = errors.New("value decompress error")
	ErrInvalidKey       = errors.New("invalid key")
	ErrReadKey          = errors.New("key read error")
	ErrReadDoc          = errors.New("doc read error")
	ErrFileExists       = errors.New("file already exists")
	//ErrNotSupport      = errors.New("not support for this compression type")
)

type Doc struct {
	Key     []byte
	Content []byte
}

type DocKey struct {
	KeySize     int32
	ContentSize int32
	Key         []byte
}

type Node struct {
	Size int32
	Data []byte
}

func ReadDoc(r io.Reader, doc *Doc) (int, error) {
	dc := &DocKey{}
	nr, err := readHeader(r, dc)
	if err != nil {
		return nr, err
	}

	doc.Content = make([]byte, dc.ContentSize)
	var n int
	n, err = r.Read(doc.Content)
	nr += n
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "read doc content error: %v\n", err)
		return nr, ErrReadDoc
	}
	doc.Key = dc.Key
	return nr, nil
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

func readInt32(r io.Reader, num *int32) (int, error) {
	err := binary.Read(r, binary.LittleEndian, num)
	if err != nil {
		return 0, err
	}
	return int(unsafe.Sizeof(*num)), nil
}

func Decompress(doc *Doc, compressType int) (dst *Doc, err error) {
	if NONE == compressType {
		return doc, nil
	}
	reader, err := getDecompressReader(compressType, bytes.NewReader(doc.Content))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return nil, ErrDecompressReader
	}

	var data []byte
	data, err = io.ReadAll(reader)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return nil, ErrValueDecompress
	}
	return &Doc{Key: CloneBytes(doc.Key), Content: data}, nil
}

func Compress(doc *Doc, compressType int) (dst *Doc, err error) {
	if NONE == compressType {
		return doc, nil
	}
	buf := bytes.Buffer{}
	writer, err := getCompressWriter(compressType, &buf)
	if err != nil {
		return nil, err
	}

	_, err = writer.Write(doc.Content)
	if err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	return &Doc{Key: CloneBytes(doc.Key), Content: buf.Bytes()}, nil
}

// writeDoc Write document to writer
func (doc *Doc) writeDoc(w io.Writer) (int, error) {
	var err error
	var n, nb int
	n, err = writeNode(w, doc.Key)
	if err != nil {
		return n, err
	}
	nb, err = writeNode(w, doc.Content)
	return n + nb, err
}

func writeNode(w io.Writer, data []byte) (n int, err error) {
	keySize := int32(len(data))
	err = binary.Write(w, binary.LittleEndian, keySize)
	if err != nil {
		return 0, err
	}
	n, err = w.Write(data)
	return n + int(keySize), err
}

func readNode(reader io.Reader, node *Node) (nr int, err error) {
	nr, err = readInt32(reader, &node.Size)
	if err == io.EOF {
		return nr, err
	}
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "read int error: %v\n", err)
		return nr, ErrReadKey
	}
	if node.Size < 0 {
		return nr, ErrReadKey
	}
	node.Data = make([]byte, node.Size)
	var n int
	n, err = reader.Read(node.Data)
	nr += n

	if err == io.EOF && int32(n) < node.Size {
		return nr, InvalidDocumentFound
	}

	if err != nil && err != io.EOF {
		_, _ = fmt.Fprintf(os.Stderr, "read node data error: %v\n", err)
		return nr, ErrReadDoc
	}
	return nr, nil
}

func readHeader(reader io.Reader, doc *DocKey) (int, error) {
	node := &Node{}
	nr, err := readNode(reader, node)
	if err != nil {
		return nr, err
	}
	if int32(len(string(doc.Key))) > KeySizeLimit {
		return nr, ErrInvalidKey
	}
	n, err := readInt32(reader, &doc.ContentSize)
	nr += n
	doc.Key = node.Data
	doc.KeySize = node.Size
	return nr, err
}
