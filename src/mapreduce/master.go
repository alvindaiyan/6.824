package mapreduce

import "container/list"
import 	"fmt"


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
	startWorker := func(workerAddr string, idx int, jobArgs DoJobArgs) {
		var reply DoJobReply
		ok := call(workerAddr, "Worker.DoJob", jobArgs, &reply)
		if ok {
			mr.DoneChannel <- true
			mr.registerChannel <- workerAddr
		} else {
			// todo not successful?
		}
	}

	nextWorker := func() string {
		return <-mr.registerChannel
	}

	// do the mappers
	for i:= 0; i < mr.nMap; i++ {
		workerAddr := nextWorker()
		jobArg := DoJobArgs{mr.file, Map, i, mr.nReduce }
		go startWorker(workerAddr, i, jobArg)
	}

	// lock to wait mapper to finish
	for i:=0; i < mr.nMap; i++ {
		<- mr.DoneChannel
	}

	for i:= 0; i < mr.nMap; i++ {
		workerAddr := nextWorker()
		jobArg := DoJobArgs{mr.file, Reduce, i, mr.nMap }
		go startWorker(workerAddr, i, jobArg)
	}


	// wait for reducers to finish
	for i:=0; i < mr.nReduce; i++ {
		<- mr.DoneChannel
	}

	return mr.KillWorkers()
}
