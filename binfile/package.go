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

func searchFiles(root string, ch, stop chan interface{}, pattern *regexp.Regexp) {
	if Debug {
		fmt.Printf("searching files in %s\n", root)
	}
	defer func() {
		ch <- nil
	}()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// root will not be dir if err is not nil
			// stopCh processing dir if read dir error
			debug("skip processing dir %s: %v\n", path, err)
			return fs.SkipDir
		}
		// process only regular files (d is not nil if err is nil)
		// filter out files those not match the pattern
		if path == "" || !fs.FileMode.IsRegular(d.Type()) || pattern != nil && !pattern.MatchString(path) {
			if Debug {
				fmt.Printf("%s skipped\n", path)
			}
			return nil
		}
		// files are queue to processed
		// and stopCh process
		select {
		case ch <- path:
			return nil
		case <-stop:
			// stopCh walking when workers stopped
			return errWorkersStopped
		}
	})

	if err != nil && !errors.Is(err, errWorkersStopped) {
		_, _ = fmt.Fprintf(os.Stderr, "search root error: %v\n", err)
	}
	fmt.Println("root search done")
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
	doc.Key = []byte(parts[len(parts)-1])
	doc.Content = content
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
		if _, err := dw.Write(doc); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[%d] worker error: %v\n", no, err)
			break
		}
	}
}
