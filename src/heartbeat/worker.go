package heartbeat

import (
	"fmt"
	"time"
)

type WorkerJob int64

const (
	Idle WorkerJob = iota
	MapSchedule
	MapInProgress
	ReduceSchedule
	ReduceInProgress
)

var jobNames = map[WorkerJob]string{
	Idle:             "Idle",
	MapSchedule:      "MapSchedule",
	MapInProgress:    "MapInProgress",
	ReduceSchedule:   "ReduceSchedule",
	ReduceInProgress: "ReduceInProgress",
}

func (j WorkerJob) String() string {
	if name, ok := jobNames[j]; ok {
		return name
	}
	return "unknown"
}

func StartProcessing(c *Coordinator, close chan bool) {
	state := WorkerState{}
	var heartBeatReceived <-chan WorkerState

	startMapJob := make(chan bool, 1)
	var mapFinished <-chan WorkerState

	startReduceJob := make(chan bool, 1)
	var reduceFinished <-chan WorkerState

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			heartBeatReceived = callHb(c, state)
		case state = <-heartBeatReceived:
			heartBeatReceived = nil
			fmt.Println("This is received heart beat", state.JobStatus, state.Timestamp)
			// do job scheduling here
			if state.JobStatus == MapSchedule {
				startMapJob <- true
			} else if state.JobStatus == ReduceSchedule {
				startReduceJob <- true
			}
		case <-startMapJob:
			state.JobStatus = MapInProgress
			mapFinished = doMap(state)
		case state = <-mapFinished:
			mapFinished = nil
			c.JobFinished(state)
		case <-startReduceJob:
			state.JobStatus = ReduceInProgress
			reduceFinished = doReduce(state)
		case state = <-reduceFinished:
			reduceFinished = nil
		case <-close:
			fmt.Print("at close")
			break
		}
	}
}

func doMap(state WorkerState) <-chan WorkerState {
	result := make(chan WorkerState)
	go func() {
		fmt.Println("Starting doing map job", time.Now())
		<-time.After(time.Second * 12)
		fmt.Println("finished map job", time.Now())
		state.JobStatus = Idle

		for i := 0; i < state.MapJob.nReducers; i++ {
			arr, ok := state.MapJob.outPuts[i]
			if ok == false {
				arr = make([]string, 0)
			}
			arr = append(arr, fmt.Sprintf("%v-%v", state.MapJob.file, i+1))
			state.MapJob.outPuts[i] = arr
		}

		result <- state
		defer close(result)
	}()
	return result
}

func doReduce(state WorkerState) <-chan WorkerState {
	result := make(chan WorkerState)
	go func() {
		fmt.Println("Starting doing reduce job", time.Now())
		<-time.After(time.Second * 22)
		fmt.Println("finished reduce job", time.Now())
		state.JobStatus = Idle
		result <- state
		defer close(result)
	}()
	return result
}

func callHb(c *Coordinator, state WorkerState) <-chan WorkerState {
	hb := make(chan WorkerState)
	go func() {
		fmt.Println("Calling heartbeat", time.Now())
		<-time.After(time.Second * 2)
		state = c.Schedule(state)
		hb <- state
		defer close(hb)
	}()
	return hb
}
