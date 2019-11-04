package mapreduce

import (
	"fmt"
	"log"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	if phase == mapPhase {
		var waitGroup sync.WaitGroup

		waitGroup.Add(ntasks)
		log.Println("waitGroup=", waitGroup)

		for i := 0; i < ntasks; i++ {
			select {
			case workerAddr := <-registerChan:
				go func(idx int, waitGroup *sync.WaitGroup) {
					call(workerAddr, "Worker.DoTask", DoTaskArgs{
						JobName:       jobName,
						File:          mapFiles[idx],
						Phase:         mapPhase,
						TaskNumber:    idx,
						NumOtherPhase: n_other,
					}, nil)
					registerChan <- workerAddr
					waitGroup.Done()
				}(i, &waitGroup)
			}
		}

		log.Println("before wait...")
		log.Println("waitGroup=", waitGroup)
		waitGroup.Wait()
		log.Println("after wait...")
	} else {
		var waitGroup sync.WaitGroup

		waitGroup.Add(ntasks)
		log.Println("waitGroup=", waitGroup)

		for i := 0; i < ntasks; i++ {
			select {
			case workerAddr := <-registerChan:
				go func(idx int, waitGroup *sync.WaitGroup) {
					call(workerAddr, "Worker.DoTask", DoTaskArgs{
						JobName:       jobName,
						File:          "",
						Phase:         reducePhase,
						TaskNumber:    idx,
						NumOtherPhase: n_other,
					}, nil)
					registerChan <- workerAddr
					waitGroup.Done()
				}(i, &waitGroup)
			}
		}

		waitGroup.Wait()
	}

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
