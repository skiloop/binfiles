package binfile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/skiloop/binfiles/workers"
)

type fileRepack struct {
	docCh      chan *Doc
	filenameCh chan string
	stopSeeder chan interface{}
	reader     SeekReader
	limit      int
	target     string
	pt         int
	tt         int
	st         int
	split      int
	idx        atomic.Int32
}

func (r *fileRepack) nextBinWriter(optimized bool) BinWriter {
	no := r.idx.Add(1)
	filename := fmt.Sprintf("%s.%d", r.target, no)
	writer := NewBinWriter(filename, r.tt)
	if optimized {
		return writer
	}
	return &oldBinWriter{
		binWriter:     writer.(*binWriter),
		oldCompressor: oldCompressor{},
	}
}

func (r *fileRepack) seeder() {
	LogInfo("reader starts")
	count := 0
	running := true
	for running {
		offset, _ := r.reader.Seek(0, io.SeekCurrent)
		doc, err := r.reader.Read(true)
		if err == io.EOF {
			break
		}
		if errors.Is(err, ErrValueDecompress) {
			LogError("doc read error at %d: %v\n", offset, err)
			continue
		}
		if err != nil {
			LogError("doc read error at %d: %v\n", offset, err)
			break
		}
		if doc == nil {
			break
		}

		// 确保数据不丢失：要么发送成功，要么收到停止信号
		select {
		case r.docCh <- doc:
			count += 1
			if r.limit > 0 && count >= r.limit {
				running = false
			}
		case <-r.stopSeeder:
			running = false
		}
	}
	LogInfo("reader done with %d documents\n", count)
	r.docCh <- nil
}

func (r *fileRepack) merge(stopCh chan interface{}) {
	defer func() {
		stopCh <- nil
	}()
	fw, err := os.OpenFile(r.target, writerFileFlag, 0644)
	if err != nil {
		LogError("fail to open file %s: %v\n", r.target, err)
		return
	}
	defer closeWriter(fw, r.target)

	cw, err := getCompressor(r.pt, fw)
	if err != nil {
		return
	}
	defer closeWriter(cw, "compressor")

	var rdr *os.File
	failed := false
	var nw int64
	count := 0
	for {
		filename := <-r.filenameCh
		if filename == workerEndFlag {
			break
		}
		if failed {
			continue
		}
		rdr, err = os.OpenFile(filename, os.O_RDONLY, 0644)
		if err != nil {
			LogError("fail to open file %s: %v\n", filename, err)
			failed = true
			continue
		}
		LogInfo("merging %s\n", filename)
		nw, err = io.Copy(cw, rdr)
		_ = rdr.Close()
		if err != nil {
			LogError("fail to append file %s: %v\n", filename, err)
			failed = true
			continue
		}
		count += 1
		_ = os.Remove(filename)
		if Debug {
			LogInfo("%s merged with %d bytes\n", filename, nw)
		}
	}
	LogInfo("merger done with %d files\n", count)

}

func (r *fileRepack) worker(no int, optimized bool) {
	LogInfo("worker %d started\n", no)
	var err error
	rp := r.nextBinWriter(optimized)
	err = rp.Open()
	if err != nil {
		return
	}
	init := 100 * no
	count := int64(init)
	docs := 0
	for {
		doc := <-r.docCh
		if doc == nil {
			r.docCh <- doc
			r.filenameCh <- rp.Filename()
			break
		}
		if Verbose {
			LogInfo("[%d]package %s\n", no, doc.Key)
		}
		_, err = rp.Write(doc)
		if err != nil {
			LogError("[%d]write error: %s, %v\n", no, doc.Key, err)
			continue
		}
		docs += 1
		count += 1

		if r.split > 0 && count%int64(r.split) == 0 {
			LogInfo("[%d] %s done with %d docs\n", no, rp.Filename(), docs)
			_ = rp.Close()
			r.filenameCh <- rp.Filename()
			rp = r.nextBinWriter(optimized)
			err = rp.Open()
			if err != nil {
				LogError("[%d]failed to get next packager: %v\n", no, err)
				break
			}
			docs = 0
		}
	}
	count -= int64(init)
	LogInfo("[%d] %s done with %d docs\n", no, rp.Filename(), docs)
	_ = rp.Close()
	LogInfo("[%d]fileWorker done with %d docs\n", no, count)
}

func (r *fileRepack) start(source string, workerCount int, optimized bool) error {
	fn, err := os.OpenFile(source, os.O_RDONLY, 0644)
	if err != nil {
		LogError("failed to open %s: %v\n", source, err)
		return err
	}
	r.reader = NewSeeker(fn, r.st)
	defer func(r io.Closer) {
		_ = r.Close()
	}(r.reader)

	// 增加channel容量，避免阻塞
	r.docCh = make(chan *Doc, workerCount*2)
	r.filenameCh = make(chan string, workerCount*2)
	r.stopSeeder = make(chan interface{})

	waitMergerCh := make(chan interface{})
	go r.merge(waitMergerCh)

	workers.RunJobs(workerCount, r.stopSeeder, func(no int) {
		r.worker(no, optimized)
	}, r.seeder)

	r.filenameCh <- workerEndFlag
	<-waitMergerCh
	return nil
}
