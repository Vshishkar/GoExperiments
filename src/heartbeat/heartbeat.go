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

func (j WorkerJob) String() string {
	switch j {
	case Idle:
		return "Idle"
	case MapSchedule:
		return "MapSchedule"
	case MapInProgress:
		return "MapInProgress"
	case ReduceSchedule:
		return "ReduceSchedule"
	case ReduceInProgress:
		return "ReduceInProgress"
	}
	return "unknown"
}

type Heartbeat struct {
	timestamp time.Time
	job       WorkerJob
}

func Execute() {
	state := Heartbeat{}
	var nextHeartbeat time.Time
	var heartBeatReceived <-chan Heartbeat

	startMapJob := make(chan bool, 1)
	var mapFinished <-chan Heartbeat

	startReduceJob := make(chan bool, 1)
	var reduceFinished <-chan Heartbeat

	for {
		var hbDelay time.Duration
		if now := time.Now(); nextHeartbeat.After(now) {
			hbDelay = nextHeartbeat.Sub(now)
		}
		callHeartbeat := time.After(hbDelay)
		select {
		case <-callHeartbeat:
			heartBeatReceived = callHb(state)
			nextHeartbeat = time.Now().Add(time.Second * 10)
		case state = <-heartBeatReceived:
			heartBeatReceived = nil
			fmt.Println("This is received heart beat", state.job, state.timestamp)
			// do job scheduling here
			if state.job == MapSchedule {
				startMapJob <- true
			} else if state.job == ReduceSchedule {
				startReduceJob <- true
			}
		case <-startMapJob:
			state.job = MapInProgress
			mapFinished = doMap(state)
		case state = <-mapFinished:
			mapFinished = nil
		case <-startReduceJob:
			state.job = ReduceInProgress
			reduceFinished = doReduce(state)
		case state = <-reduceFinished:
			reduceFinished = nil
		}

	}
}

func doMap(state Heartbeat) <-chan Heartbeat {
	result := make(chan Heartbeat)
	go func() {
		fmt.Println("Starting doing map job", time.Now())
		<-time.After(time.Second * 12)
		fmt.Println("finished map job", time.Now())
		state.job = Idle
		result <- state
		defer close(result)
	}()
	return result
}

func doReduce(state Heartbeat) <-chan Heartbeat {
	result := make(chan Heartbeat)
	go func() {
		fmt.Println("Starting doing reduce job", time.Now())
		<-time.After(time.Second * 22)
		fmt.Println("finished reduce job", time.Now())
		state.job = Idle
		result <- state
		defer close(result)
	}()
	return result
}

func callHb(state Heartbeat) <-chan Heartbeat {
	hb := make(chan Heartbeat)
	go func() {
		fmt.Println("Calling heartbeat", time.Now())
		<-time.After(time.Second * 2)
		state.timestamp = time.Now()

		if state.job == MapInProgress || state.job == ReduceInProgress {
			hb <- state
			return
		}

		if state.job == Idle {
			if rand.Intn(2) == 1 {
				state.job = MapSchedule
			} else {
				state.job = ReduceSchedule
			}
		}
		hb <- state
		defer close(hb)
	}()
	return hb
}
