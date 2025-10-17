package binfile

import (
	"io"
	"math"
	"os"
	"sync/atomic"

	"github.com/skiloop/binfiles/workers"
)

type docRepack struct {
	docCh       chan *Doc
	stopCh      chan interface{}
	limit       int
	source      string
	target      string
	contentOnly bool
	pt          int
	tt          int
	st          int
	split       int
	fileSize    int64
	pos         atomic.Int64
	step        int64
}

type DocWriterCloser interface {
	DocWriter
	io.Closer
}

func (r *docRepack) worker(no int) {

	end := r.pos.Add(r.step)
	offset := end - r.step
	LogInfo("[%d] worker starts on %d to %d\n", no, offset, end)
	br, err := NewBinReader(r.source, r.st)
	if err != nil {
		return
	}
	reader, _ := br.(*binReader)
	count := 0
	var doc *Doc
	_ = reader.resetOffset(offset)
	for {
		doc, err = reader.docSeeker.Read(true)
		if err != nil {
			pos, dc := reader.next(offset, end, -1, -1, nil)
			if dc == nil {
				LogInfo("[%d] no more doc after %d\n", no, offset)
				break
			}
			offset, doc = pos, dc
		} else {
			offset, err = reader.docSeeker.Seek(0, io.SeekCurrent)
			if err != nil {
				break
			}
		}
		if offset > end {
			break
		}
		// Safely send to the channel
		select {
		case r.docCh <- doc:
			count++
		case <-r.stopCh: // Handle stop signal
			LogInfo("[%d] worker stopped\n", no)
			// tell other workers to stop
			r.stopCh <- nil
			break
		}
	}
	LogInfo("[%d] worker done with %d documents\n", no, count)

}
func (r *docRepack) createWriter(optimized bool) (DocWriterCloser, error) {
	if r.contentOnly {
		bw := newConDocFileWriter(r.target, r.pt)
		if err := bw.Open(); err != nil {
			return nil, err
		}
		return bw, nil
	}
	var bw BinWriter
	var err error
	if optimized {
		bw, err = NewCCBinWriter(r.target, r.pt, r.tt)
	} else {
		bw, err = NewOldCCBinWriter(r.target, r.pt, r.tt)
	}
	if err != nil {
		return nil, err
	}
	if err := bw.Open(); err != nil {
		return nil, err
	}
	return bw, nil
}

func (r *docRepack) merge(optimized bool) {

	bw, err := r.createWriter(optimized)
	if err != nil {
		LogError("fail to create writer: %v\n", err)
		return
	}
	defer func() {
		_ = bw.Close()
	}()
	defer func() {
		r.stopCh <- nil
	}()
	LogInfo("merger starts")
	count := 0
	var doc *Doc
	for {
		select {
		case doc = <-r.docCh:
		case <-r.stopCh:
			LogInfo("merger got stop signal")
			break
		}
		if doc == nil {
			break
		}
		// LogInfo("merge doc: %s, %d -> %d\n", string(doc.Key), len(doc.Content), len(doc.Content))
		_, err = bw.Write(doc)
		if err != nil {
			LogError("fail to write %s: %v\n", doc.Key, err)
			continue
		}
		count += 1
	}
	LogInfo("merger done with %d files\n", count)
}

func getFileSize(filename string) (int64, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

func (r *docRepack) start(workerCount int, optimized bool) (err error) {
	// set step
	r.fileSize, err = getFileSize(r.source)
	if err != nil {
		return err
	}
	r.step = int64(math.Ceil(float64(r.fileSize) / float64(workerCount)))
	// create channel
	r.docCh = make(chan *Doc, workerCount+3)
	r.stopCh = make(chan interface{})
	// start run
	go r.merge(optimized)
	workers.RunJobs(workerCount, nil, r.worker, nil)
	// tell merger to finish
	r.docCh <- nil
	<-r.stopCh
	return nil
}
