package workers

import (
	"fmt"
	"sync"
	"time"
)

type JobsRunner struct {
	WorkCount int
	StopCh    chan interface{} // channel to interrupt seeder
	Task      func(no int)
	Seeder    func()
}

func (jr *JobsRunner) Run() {
	start := time.Now()
	wg := sync.WaitGroup{}
	sw := sync.WaitGroup{}
	for no := 0; no < jr.WorkCount; no++ {
		wg.Add(1)
		go func(no int) {
			defer wg.Done()
			jr.Task(no)
		}(no)
	}
	if jr.Seeder != nil {
		sw.Add(1)
		go func() {
			defer sw.Done()
			jr.Seeder()
		}()
	}
	// wait for all task done
	wg.Wait()
	select {
	case jr.StopCh <- nil:
	default:
	}
	sw.Wait()
	usage := time.Now().Sub(start)
	fmt.Printf("all tasks done in %s\n", usage.String())
}

func RunJobs(workerCount int, stopCh chan interface{}, task func(no int), seeder func()) {
	jr := JobsRunner{
		WorkCount: workerCount,
		StopCh:    stopCh,
		Task:      task,
		Seeder:    seeder,
	}
	jr.Run()
}
