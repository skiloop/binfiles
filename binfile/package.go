package binfile

import (
	"errors"
	"fmt"
	"github.com/skiloop/binfiles/workers"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Package files to bin file
func Package(option *PackageOption, bw BinWriter) (err error) {
	if err = bw.Open(); err != nil {
		return err
	}
	var pattern *regexp.Regexp
	if len(option.Pattern) > 0 {
		if pattern, err = regexp.Compile(option.Pattern); err != nil {
			return err
		}
	}

	ch := make(chan interface{}, option.WorkerCount*3)
	stopCh := make(chan interface{})

	workers.RunJobs(option.WorkerCount, stopCh, func(no int) {
		packageWorker(ch, option.InputCompress, no, bw)
	}, func() {
		searchFiles(option.Path, ch, stopCh, pattern)
	})
	fmt.Printf("%s packaging done\n", option.Path)
	return nil
}

func searchFiles(path string, ch, stop chan interface{}, pattern *regexp.Regexp) {
	if Debug {
		fmt.Printf("searching files in %s\n", path)
	}
	err := filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// path will not be dir if err is not nil
			// stop processing dir if read dir error
			fmt.Printf("skip processing dir %s: %v\n", path, err)
			return fs.SkipDir
		}
		// process only regular files (d is not nil if err is nil)
		// filter out files those not match the pattern
		if path == "" || !fs.FileMode.IsRegular(d.Type()) || pattern != nil && !pattern.MatchString(path) {
			return nil
		}
		// files are queue to processed
		// and stop process
		select {
		case ch <- path:
			return nil
		case <-stop:
			// stop walking when workers stopped
			return workersStopped
		}
	})
	ch <- nil
	if err != nil && !errors.Is(err, workersStopped) {
		_, _ = fmt.Fprintf(os.Stderr, "search path error: %v\n", err)
	}
	fmt.Println("path search done")
}

func readContent(path string, compress int) ([]byte, error) {
	in, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = in.Close()
	}()
	reader, err := getDecompressReader(compress, in)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "decompress reader error: %v\n", err)
		return nil, err
	}
	var data []byte
	data, err = io.ReadAll(reader)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "decompress error: %v\n", err)
		return nil, err
	}
	return data, nil
}

func readDoc(path string, compress int) (*Doc, error) {
	parts := strings.Split(path, "/")
	content, err := readContent(path, compress)
	if nil == content {
		return nil, err
	}
	doc := &Doc{}
	doc.Key = parts[len(parts)-1]
	doc.Content = string(content)
	return doc, nil
}

func packageWorker(ch chan interface{}, compress, no int, dw BinWriter) {
	for {
		src := <-ch
		if src == nil {
			if Verbose {
				fmt.Printf("worker %d stopped\n", no)
			}
			ch <- nil
			break
		}
		path, ok := src.(string)
		if !ok {
			continue
		}

		if Verbose {
			fmt.Printf("[%d] process file %s\n", no, path)
		}

		doc, err := readDoc(path, compress)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[%d] read file %s error: %v\n", no, path, err)
			continue
		}
		if err := dw.Write(doc); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[%d] worker error: %v\n", no, err)
			break
		}
	}
}
