package binfile

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"
)

var (
	ErrDecompressReader = errors.New("fail to get decompress reader")
	//ErrCompressWriter   = errors.New("fail to get compress writer")
	ErrValueDecompress = errors.New("value decompress error")
	ErrInvalidKey      = errors.New("invalid key")
	ErrReadKey         = errors.New("key read error")
	ErrReadDoc         = errors.New("doc read error")
	ErrFileExists      = errors.New("file already exists")
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
	if dc.ContentSize > MaxDocSize || dc.ContentSize <= 0 {
		return nr, ErrReadDoc
	}

	// 使用内存池获取缓冲区，预分配合适的大小
	initialSize := int(dc.ContentSize)
	if initialSize < 512 {
		initialSize = 512
	}

	doc.Content = make([]byte, 0, initialSize)

	// 预分配足够的空间
	if int32(cap(doc.Content)) < dc.ContentSize {
		doc.Content = make([]byte, 0, int(dc.ContentSize))
	}

	var n int
	for {
		if len(doc.Content) == cap(doc.Content) {
			doc.Content = append(doc.Content, 0)[:len(doc.Content)]
		}
		if int32(cap(doc.Content)) > dc.ContentSize {
			n = int(dc.ContentSize)
		} else {
			n = cap(doc.Content)
		}
		n, err = r.Read(doc.Content[len(doc.Content):n])
		doc.Content = doc.Content[:len(doc.Content)+n]
		if err != nil || int32(len(doc.Content)) >= dc.ContentSize {
			break
		}
	}

	if err != nil && io.EOF != err {
		_, _ = fmt.Fprintf(os.Stderr, "read doc content error: %v\n", err)
		return nr, ErrReadDoc
	}
	doc.Key = dc.Key
	return nr, nil
}

//
//func readBytes(r io.Reader, size int) ([]byte, error) {
//	keyBuf := make([]byte, size)
//	n, err := r.Read(keyBuf)
//	if err != nil {
//		return nil, err
//	}
//	if n != size {
//		return nil, ErrReadKey
//	}
//	return keyBuf, nil
//}

func readInt32(r io.Reader, num *int32) (int, error) {
	err := binary.Read(r, binary.LittleEndian, num)
	if err != nil {
		return 0, err
	}
	return int(unsafe.Sizeof(*num)), nil
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
	if node.Size < 0 || node.Size > MaxDocSize || node.Size > KeySizeLimit {
		_, _ = fmt.Fprintf(os.Stderr, "read node size error: %d\n", node.Size)
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
