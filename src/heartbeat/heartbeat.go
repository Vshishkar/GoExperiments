package heartbeat

import (
	"fmt"
	"math/rand"
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

type Heartbeat struct {
	Timestamp time.Time
	Job       WorkerJob
}

func Execute() {
	state := &Heartbeat{}
	var heartBeatReceived <-chan *Heartbeat

	startMapJob := make(chan bool, 1)
	var mapFinished <-chan *Heartbeat

	startReduceJob := make(chan bool, 1)
	var reduceFinished <-chan *Heartbeat

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			heartBeatReceived = callHb(state)
		case state = <-heartBeatReceived:
			heartBeatReceived = nil
			fmt.Println("This is received heart beat", state.Job, state.Timestamp)
			// do job scheduling here
			if state.Job == MapSchedule {
				startMapJob <- true
			} else if state.Job == ReduceSchedule {
				startReduceJob <- true
			}
		case <-startMapJob:
			state.Job = MapInProgress
			mapFinished = doMap(state)
		case state = <-mapFinished:
			mapFinished = nil
		case <-startReduceJob:
			state.Job = ReduceInProgress
			reduceFinished = doReduce(state)
		case state = <-reduceFinished:
			reduceFinished = nil
		}

	}
}

func doMap(state *Heartbeat) <-chan *Heartbeat {
	result := make(chan *Heartbeat)
	go func() {
		fmt.Println("Starting doing map job", time.Now())
		<-time.After(time.Second * 12)
		fmt.Println("finished map job", time.Now())
		state.Job = Idle
		result <- state
		defer close(result)
	}()
	return result
}

func doReduce(state *Heartbeat) <-chan *Heartbeat {
	result := make(chan *Heartbeat)
	go func() {
		fmt.Println("Starting doing reduce job", time.Now())
		<-time.After(time.Second * 22)
		fmt.Println("finished reduce job", time.Now())
		state.Job = Idle
		result <- state
		defer close(result)
	}()
	return result
}

func callHb(state *Heartbeat) <-chan *Heartbeat {
	hb := make(chan *Heartbeat)
	go func() {
		fmt.Println("Calling heartbeat", time.Now())
		<-time.After(time.Second * 2)
		state.Timestamp = time.Now()

		if state.Job == MapInProgress || state.Job == ReduceInProgress {
			hb <- state
			return
		}

		if state.Job == Idle {
			if rand.Intn(2) == 1 {
				state.Job = MapSchedule
			} else {
				state.Job = ReduceSchedule
			}
		}
		hb <- state
		defer close(hb)
	}()
	return hb
}
