package binfile

import (
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/skiloop/binfiles/binfile/filelock"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

const endFlag = ""

type docWriter struct {
	binWriterFile
	fn *os.File
	mu sync.Mutex
}

var workersStopped = errors.New("workers stopped")

func (dw *docWriter) checkAndOpen() error {
	if dw.file != nil {
		return nil
	}
	fn, err := os.OpenFile(dw.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0x644)
	if err != nil {
		return err
	}
	dw.fn = fn
	dw.file = fn
	return nil
}

func (dw *docWriter) lock() error {
	dw.mu.Lock()
	err := filelock.Lock(*dw.fn)
	if err == nil {
		return nil
	}
	dw.mu.Unlock()
	return err
}

func (dw *docWriter) unlock() error {
	err := filelock.UnLock(*dw.fn)
	if err == nil {
		dw.mu.Unlock()
		return nil
	}
	return err
}

func (dw *docWriter) Open() error {
	return dw.checkAndOpen()
}

// Package files to bin file
func (dw *docWriter) Package(option *PackageOption) (err error) {
	if err = dw.Open(); err != nil {
		return err
	}
	var pattern *regexp.Regexp
	if len(option.Pattern) > 0 {
		if pattern, err = regexp.Compile(option.Pattern); err != nil {
			return err
		}
	}

	ch := make(chan string, option.WorkerCount*3)
	stopped := make(chan bool)

	go dw.startPackageWorkers(ch, stopped, option.WorkerCount, option.InputCompress)
	err = filepath.WalkDir(option.Path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// path will not be dir if err is not nil
			// stop processing dir if read dir error
			fmt.Printf("skip processing dir %s: %v\n", path, err)
			return fs.SkipDir
		}
		// process only regular files (d is not nil if err is nil)
		// filter out files those not match the pattern
		if !fs.FileMode.IsRegular(d.Type()) || pattern != nil && !pattern.MatchString(path) {
			return nil
		}
		// files are queue to processed
		// and stop process
		select {
		case ch <- path:
			return nil
		case <-stopped:
			// stop walking when workers stopped
			return workersStopped
		}
	})

	if err == nil {
		ch <- endFlag
		fmt.Println("path is walked, wait for all workers")
		<-stopped
		fmt.Printf("%s packaging done\n", option.Path)
	} else {
		fmt.Println("processing failed")
		if errors.Is(err, workersStopped) {
			fmt.Println(err.Error())
		} else {
			fmt.Printf("%v\n", err)
			select {
			case ch <- endFlag:
			case <-stopped:
			}
		}
	}
	return err
}

func (dw *docWriter) startPackageWorkers(ch chan string,
	stopped chan bool, workCount int, compress int) {
	wg := sync.WaitGroup{}
	for workCount > 0 {
		wg.Add(1)
		go func(no int) {
			defer wg.Done()
			for {
				path := <-ch
				if path == endFlag {
					if Verbose {
						fmt.Printf("worker %d stopped\n", no)
					}
					ch <- path
					break
				}
				if Verbose {
					fmt.Printf("[%d] process file %s\n", no, path)
				}
				if err := dw.writeFile(path, compress); err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "worker %d stopped with error: %v\n", no, err)
					break
				}
			}
		}(workCount)
		workCount--
	}
	fmt.Println("wait for all workers")
	wg.Wait()
	stopped <- true
	fmt.Println("all workers stopped")
}

func readContent(path string, compress int) []byte {
	in, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer func() {
		_ = in.Close()
	}()
	var reader io.Reader
	switch compress {
	case GZIP:
		reader, err = gzip.NewReader(in)
	case NONE:
		reader = in
	default:
		_, _ = fmt.Fprintf(os.Stderr, "unsupported compression type: %d\n", compress)
		return nil
	}
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "decompress error: %v\n", err)
		return nil
	}
	var data []byte
	data, err = io.ReadAll(reader)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "decompress error: %v\n", err)
		return nil
	}
	return data
}

func (dw *docWriter) writeFile(path string, compress int) error {

	parts := strings.Split(path, "/")
	content := readContent(path, compress)
	if nil == content {
		return nil
	}
	doc := &Doc{CompressType: dw.compressType}
	doc.Key = parts[len(parts)-1]
	doc.Content = string(content)
	return dw.Write(doc)
}

func (dw *docWriter) Write(doc *Doc) error {
	var err error
	if err = dw.lock(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "mu file error: %v\n", err)
		return err
	}
	defer func() {
		_ = dw.unlock()
	}()
	if doc.CompressType != dw.compressType {
		doc.CompressType = dw.compressType
	}
	if err = doc.writeDoc(dw.file); Verbose && err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return err
	}
	return nil
}
