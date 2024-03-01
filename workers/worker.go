package workers

import (
	"fmt"
	"time"
)

type JobsRunner struct {
	WorkCount int
	StopCh    chan interface{}
	Async     bool // seeder async
	Task      func(no int)
	Seeder    func()
}

func (jr *JobsRunner) Run() {
	start := time.Now()

	for no := 0; no < jr.WorkCount; no++ {
		go jr.Task(no)
	}
	if jr.Seeder != nil {
		if jr.Async {
			go jr.Seeder()
		} else {
			jr.Seeder()
		}
	}
	// wait for all task done
	for no := 0; no < jr.WorkCount; no++ {
		<-jr.StopCh
	}
	usage := time.Now().Sub(start)
	fmt.Printf("all tasks done in %s\n", usage.String())
}

func RunJobs(workerCount int, stopCh chan interface{}, async bool, task func(no int), seeder func()) {
	jr := JobsRunner{WorkCount: workerCount, StopCh: stopCh, Async: async, Task: task, Seeder: seeder}
	jr.Run()
}
