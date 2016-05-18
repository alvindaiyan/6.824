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
	log.Println("nmap ", mr.nMap, "nreducer", mr.nReduce, len(mr.DoneChannel))
	nextWorker := func() string {
		return <-mr.registerChannel
	}

	startWorker := func(jobArgs DoJobArgs) {
		for {
			workerAddr := nextWorker()
			fmt.Println("start new worker --> ", workerAddr)

			var reply DoJobReply
			ok := call(workerAddr, "Worker.DoJob", jobArgs, &reply)
			if ok {
				log.Println("------- worker finished work")
				mr.DoneChannel <- true
				mr.registerChannel <- workerAddr
				break
			}
		}

	}

	startMapper := func(index int) {
		fmt.Println("start mapper: ", index)
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
		log.Println("mapper ------> ", i)
		<- mr.DoneChannel
		log.Println("mapper done ------> ", i)
	}
	log.Println("<--------- mapper is done ------> ")


	for i:= 0; i < mr.nMap; i++ {
		go startReducer(i)
	}


	// wait for reducers to finish
	for i:=0; i < mr.nReduce; i++ {
		<- mr.DoneChannel
	}

	log.Println("<--------- reducer is done ------> ")


	return mr.KillWorkers()
}
