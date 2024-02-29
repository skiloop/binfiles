package workers

import (
	"fmt"
	"sync"
	"time"
)

type JobsRunner struct {
	WorkCount int
	Task      func(group *sync.WaitGroup, no int)
	Seeder    func(group *sync.WaitGroup)
}

func (jr *JobsRunner) Run() {
	start := time.Now()
	wg := new(sync.WaitGroup)
	if jr.Seeder != nil {
		go jr.Seeder(wg)
	}
	for no := 0; no < jr.WorkCount; no++ {
		go jr.Task(wg, no)
	}
	time.Sleep(10 * time.Millisecond)
	wg.Wait()
	usage := time.Now().Sub(start)
	fmt.Printf("all tasks done in %s\n", usage.String())
}

func RunJobs(workerCount int, task func(group *sync.WaitGroup, no int), seeder func(group *sync.WaitGroup)) {
	jr := JobsRunner{WorkCount: workerCount, Task: task, Seeder: seeder}
	jr.Run()
}
