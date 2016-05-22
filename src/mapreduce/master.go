package mapreduce

import "container/list"
import (
	"fmt"
	"log"
)


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	// start mappers
	done := make(chan string)
	availableWorkers := make(chan string)

	nextWorker := func() string {
		var availableWorker string
		select {
		case availableWorker = <- mr.registerChannel:
		case availableWorker = <- availableWorkers:
		}
		return availableWorker
	}

	startWorker := func(jobArgs DoJobArgs) {
		for {
			workerAddr := nextWorker()
			var reply DoJobReply
			ok := call(workerAddr, "Worker.DoJob", jobArgs, &reply)
			if ok {
				done <- workerAddr
				availableWorkers <- workerAddr
				break
			}
		}

	}

	startMapper := func(index int) {
		jobArg := DoJobArgs{mr.file, Map, index, mr.nReduce }
		startWorker(jobArg)
	}

	startReducer := func(index int) {
		jobArg := DoJobArgs{mr.file, Reduce, index, mr.nMap }
		startWorker(jobArg)
	}

	// do the mappers
	for i:= 0; i < mr.nMap; i++ {
		go startMapper(i)
	}

	// lock to wait mapper to finish
	for i:=0; i < mr.nMap; i++ {
		<- done
	}
	log.Println("<--------- mapper is done ------> ")


	for i:= 0; i < mr.nReduce; i++ {
		go startReducer(i)
	}


	// wait for reducers to finish
	for i:=0; i < mr.nReduce; i++ {
		<- done
	}

	log.Println("<--------- reducer is done ------> ")

	close(done)

	return mr.KillWorkers()
}
