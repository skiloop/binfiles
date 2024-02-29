package workers

import (
	"fmt"
	"time"
)

type JobsRunner struct {
	WorkCount int
	Task      func(no int)
	Seeder    func()
}

func (jr *JobsRunner) Run(stopCh chan interface{}) {
	start := time.Now()
	if jr.Seeder != nil {
		go jr.Seeder()
	}
	for no := 0; no < jr.WorkCount; no++ {
		go jr.Task(no)
	}
	// wait for all task done
	for no := 0; no < jr.WorkCount; no++ {
		<-stopCh
	}
	usage := time.Now().Sub(start)
	fmt.Printf("all tasks done in %s\n", usage.String())
}

func RunJobs(workerCount int, stopCh chan interface{}, task func(no int), seeder func()) {
	jr := JobsRunner{WorkCount: workerCount, Task: task, Seeder: seeder}
	jr.Run(stopCh)
}
