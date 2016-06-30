package mapreduce

import (
	"fmt"
	"log"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	log.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)

	// input file name: mr.files[task]
	// task task
	done := make(chan bool)

	startWorker := func(index int) {
		for {
			workerAddr := <-mr.registerChannel
			jobArgs := DoTaskArgs{mr.jobName, mr.files[index], phase, index, nios}
			ok := call(workerAddr, "Worker.DoTask", jobArgs, new(struct{}))
			log.Println("Register size -----> ", len(mr.registerChannel))
			if ok {
				done <- true
				mr.registerChannel <- workerAddr
				break
			}
		}

	}

	// do the mappers
	for i:= 0; i < ntasks; i++ {
		go startWorker(i)

	}

	// lock to wait mapper to finish
	for i:=0; i < ntasks; i++ {
		 <-done
	}

	fmt.Printf("Schedule: %v phase done\n", phase)

}
