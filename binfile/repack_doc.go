package binfile

import (
	"fmt"
	"github.com/skiloop/binfiles/workers"
	"io"
	"math"
	"os"
	"sync/atomic"
)

type docRepack struct {
	docCh    chan *Doc
	stopCh   chan interface{}
	limit    int
	source   string
	target   string
	pt       int
	tt       int
	st       int
	split    int
	fileSize int64
	pos      atomic.Int64
	step     int64
}

func (r *docRepack) worker(no int) {

	end := r.pos.Add(r.step)
	offset := end - r.step
	fmt.Printf("[%d] worker starts on %d to %d\n", no, offset, end)
	br, err := NewBinReader(r.source, r.st)
	if err != nil {
		return
	}
	reader, _ := br.(*binReader)
	count := 0
	offset, doc := reader.Next(&SeekOption{
		Offset:  offset,
		Pattern: "",
		KeySize: int(KeySizeLimit),
		DocSize: -1,
		End:     -1,
	})
	if doc == nil || offset >= end {
		return
	}
	_ = reader.resetOffset(offset)
	for {
		doc, err = reader.docSeeker.Read(true)
		if err != nil {
			break
		}
		offset, err = reader.docSeeker.Seek(0, io.SeekCurrent)
		if err != nil || offset > end {
			break
		}
		r.docCh <- doc
		count += 1
	}
	fmt.Printf("[%d] worker done with %d documents\n", no, count)

}

func (r *docRepack) merge() {
	defer func() {
		r.stopCh <- nil
	}()

	bw, err := NewCCBinWriter(r.target, r.pt, r.tt)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "NewCCBinWriter failed  %s: %v\n", r.target, err)
		return
	}
	if err = bw.Open(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "fail to open %s: %v\n", bw.Filename(), err)
		return
	}

	count := 0
	for {
		doc := <-r.docCh
		if doc == nil {
			break
		}
		_, err = bw.Write(doc)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "fail to write %s: %v\n", doc.Key, err)
			continue
		}
		count += 1
	}
	fmt.Printf("merger done with %d files\n", count)
}

func getFileSize(filename string) (int64, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

func (r *docRepack) start(workerCount int) (err error) {

	r.fileSize, err = getFileSize(r.source)
	if err != nil {
		return err
	}
	r.step = int64(math.Ceil(float64(r.fileSize) / float64(workerCount)))
	go r.merge()
	workers.RunJobs(workerCount, nil, r.worker, nil)
	// tell merger to finish
	r.docCh <- nil
	<-r.stopCh
	return nil
}
