package binfile

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/skiloop/binfiles/workers"
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
	LogInfo("%s packaging done\n", option.Path)
	return nil
}

func searchFiles(root string, ch, stop chan interface{}, pattern *regexp.Regexp) {
	if Debug {
		LogInfo("searching files in %s\n", root)
	}
	defer func() {
		ch <- nil
	}()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() && path != root {
			// root will not be dir if err is not nil
			// stopCh processing dir if read dir error
			debug("skip processing dir %s: %v\n", path, err)
			return fs.SkipDir
		}
		// process only regular files (d is not nil if err is nil)
		// filter out files those not match the pattern
		if path == "" || !fs.FileMode.IsRegular(d.Type()) || pattern != nil && !pattern.MatchString(path) {
			if Debug {
				LogInfo("%s skipped\n", path)
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
		LogError("search root error: %v\n", err)
	}
	LogInfo("root search done")
}

func readContent(path string, compress int) ([]byte, error) {
	in, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = in.Close()
	}()
	reader, err := getDecompressor(compress, in)
	if err != nil {
		LogError("decompress reader error: %v\n", err)
		return nil, err
	}

	// 使用内存池的缓冲区进行读取
	buf := GlobalMemoryPool.GetCompressorBuffer()
	defer GlobalMemoryPool.PutCompressorBuffer(buf)

	// 读取所有数据到缓冲区
	_, err = io.Copy(buf, reader)
	if err != nil {
		LogError("decompress error: %v\n", err)
		return nil, err
	}

	// 返回数据的副本
	return buf.Bytes(), nil
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
				LogInfo("worker %d stopped\n", no)
			}
			ch <- nil
			break
		}
		path, ok := src.(string)
		if !ok {
			continue
		}

		if Verbose {
			LogInfo("[%d] process file %s\n", no, path)
		}

		doc, err := readDoc(path, compress)
		if err != nil {
			LogError("[%d] read file %s error: %v\n", no, path, err)
			continue
		}
		if _, err := dw.Write(doc); err != nil {
			LogError("[%d] worker error: %v\n", no, err)
			break
		}
	}
}
