package binfile

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/skiloop/binfiles/workers"
)

type PackageOption struct {
	InputCompress int    `doc:"source file compression type package" default:"0"`
	WorkerCount   int    `doc:"worker count" default:"0"`
	Path          string `doc:"source path or tar file name"`
	Pattern       string `doc:"file pattern,those match will be packaged. all files include if empty"`
	TarCompress   string `doc:"tar file compression type package" default:"gzip"`
}

// Package files to bin file
func Package(option *PackageOption, bw BinWriter) (err error) {
	if err = bw.Open(); err != nil {
		return err
	}
	defer func() {
		_ = bw.Close()
	}()
	var pattern *regexp.Regexp
	if len(option.Pattern) > 0 {
		if pattern, err = regexp.Compile(option.Pattern); err != nil {
			return err
		}
	}

	ch := make(chan any, option.WorkerCount*3)
	stopCh := make(chan any)
	stat, err := os.Stat(option.Path)
	if err != nil {
		return err
	}
	defer func() {
		LogInfo("%s packaging done\n", option.Path)
	}()
	if stat.IsDir() {
		return packageDirectory(option, bw, ch, stopCh, pattern)
	} else {
		return packageTar(option, bw, ch, stopCh, pattern)
	}
}

func packageTar(option *PackageOption, bw BinWriter, ch, stop chan any, pattern *regexp.Regexp) (err error) {
	LogDebug("package tar file\n")
	workers.RunJobs(option.WorkerCount, stop, func(no int) {
		packageWorker(ch, no, bw)
	}, func() {
		tarSeeder(option, ch, stop, pattern)
	})
	LogInfo("package done\n")
	return nil
}
func tarSeeder(option *PackageOption, ch, stop chan any, pattern *regexp.Regexp) {
	LogInfo("seeder from %s starts\n", option.Path)
	in, err := os.Open(option.Path)
	defer func() {
		LogDebug("seeder stops\n")
		ch <- nil
	}()
	if err != nil {
		LogError("open tar file error: %v\n", err)
		return
	}
	defer func() {
		_ = in.Close()
	}()
	buf := GlobalMemoryPool.GetCompressorBuffer()
	defer GlobalMemoryPool.PutCompressorBuffer(buf)
	WalkTarCompressed(in, CompressionFormat(option.TarCompress), func(h *tar.Header, r io.Reader) error {
		if h.Typeflag == tar.TypeDir {
			LogDebug("skip directory %s\n", h.Name)
			return nil
		}
		key := filepath.Base(h.Name)
		if pattern != nil && !pattern.MatchString(key) {
			LogDebug("skip %s\n", h.Name)
			return nil
		}
		buf.Reset()
		_, err = io.Copy(buf, r)
		if err != nil {
			LogError("copy tar file error: %v\n", err)
			return err
		}
		select {
		case ch <- &Doc{Key: []byte(key), Content: CloneBytes(buf.Bytes())}:
			return nil
		case <-stop:
			return errWorkersStopped
		}
	})
}

func packageDirectory(option *PackageOption, bw BinWriter, ch, stop chan any, pattern *regexp.Regexp) (err error) {
	workers.RunJobs(option.WorkerCount, stop, func(no int) {
		packageWorker(ch, no, bw)
	}, func() {
		searchFiles(option.Path, ch, stop, option.InputCompress, pattern)
	})
	return nil
}

func searchFiles(root string, ch, stop chan any, compress int, pattern *regexp.Regexp) {

	LogInfo("searching files in %s\n", root)

	defer func() {
		ch <- nil
	}()
	// 使用内存池的缓冲区进行读取
	buf := GlobalMemoryPool.GetCompressorBuffer()
	defer GlobalMemoryPool.PutCompressorBuffer(buf)

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() && path != root {
			// root will not be dir if err is not nil
			// stopCh processing dir if read dir error
			LogDebug("skip processing dir %s: %v\n", path, err)
			return fs.SkipDir
		}
		// process only regular files (d is not nil if err is nil)
		// filter out files those not match the pattern
		if path == "" || !fs.FileMode.IsRegular(d.Type()) || pattern != nil && !pattern.MatchString(path) {
			LogInfo("%s skipped\n", path)
			return nil
		}
		// files are queue to processed
		// and stopCh process
		doc, err := readDoc(path, compress, buf)
		if err != nil {
			LogError("read doc error: %v\n", err)
			return nil
		}
		select {
		case ch <- doc:
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

func readContent(path string, compress int, buf *bytes.Buffer) ([]byte, error) {
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

	// 读取所有数据到缓冲区
	buf.Reset()
	_, err = io.Copy(buf, reader)
	if err != nil {
		LogError("decompress error: %v\n", err)
		return nil, err
	}

	// 返回数据的副本

	return CloneBytes(buf.Bytes()), nil
}

func readDoc(path string, compress int, buf *bytes.Buffer) (*Doc, error) {
	parts := strings.Split(path, "/")
	content, err := readContent(path, compress, buf)
	if nil == content {
		return nil, err
	}
	doc := &Doc{}
	doc.Key = []byte(parts[len(parts)-1])
	doc.Content = content
	return doc, nil
}

func packageWorker(ch chan any, no int, dw BinWriter) {
	LogDebug("worker %d starts\n", no)
	var doc any
	var count uint32
	count = 0
	for {
		doc = <-ch
		if doc == nil {
			ch <- nil
			break
		}

		if _, err := dw.Write(doc.(*Doc)); err != nil {
			LogError("[%d] worker error: %v\n", no, err)
			break
		}
		count += 1
	}
	LogInfo("worker %d done with %d docs\n", no, count)
}
