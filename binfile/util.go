package binfile

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"
)

//func open2read(filename string) (*os.File, error) {
//	fn, err := os.OpenFile(filename, os.O_RDONLY, 0644)
//	if err != nil {
//		_, _ = fmt.Fprintf(os.Stderr, "failed to open %s: %v\n", filename, err)
//		return nil, err
//	}
//	return fn, nil
//}

func CloneBytes(src []byte) []byte {
	if len(src) > 0 {
		dst := make([]byte, len(src))
		copy(dst, src)
		return dst
	}
	return []byte{}
}

func closeWriter(w io.Writer, msg string) {
	closer, ok := w.(io.Closer)
	if !ok {
		return
	}
	err := closer.Close()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s close error: %v\n", msg, err)
	}
}

func CheckFileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	//return !os.IsNotExist(err)
	return !errors.Is(err, os.ErrNotExist)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// GenerateRandomString generates a random string of the specified length.
func GenerateRandomString(n int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[r.Intn(len(letterBytes))]
	}
	return string(b)
}
