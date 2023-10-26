package heartbeat

import (
	"sync"
	"time"
)

type Coordinator struct {
	mu sync.Mutex

	files               []string
	nReducers           int
	mapJobs             map[string]int
	reducesToSchedule   [][]string
	workers             map[int]*WorkerState
	workerId            int
	reduceFinishedCount int
}

type MapJob struct {
	file      string
	nReducers int
	outPuts   map[int][]string
}

type ReduceJob struct {
	intermediateOutputs []string
}

type WorkerState struct {
	Id        int
	JobStatus WorkerJob
	MapJob    MapJob
	ReduceJob ReduceJob
	Timestamp time.Time
}

func (c *Coordinator) getId() int {
	c.workerId++
	return c.workerId
}

func (c *Coordinator) InitializeWorker() int {
	w := WorkerState{
		Id:        c.getId(),
		JobStatus: Idle,
		MapJob:    MapJob{},
		ReduceJob: ReduceJob{},
	}
	c.workers[w.Id] = &w
	return w.Id
}

func (c *Coordinator) JobFinished(s WorkerState) {
	c.mu.Lock()
	worker := c.workers[s.Id]
	worker.JobStatus = s.JobStatus
	worker.MapJob = s.MapJob
	worker.ReduceJob = s.ReduceJob

	if c.mapJobs[worker.MapJob.file] == worker.Id {
		// Map happened
		delete(c.mapJobs, worker.MapJob.file)
		for k, v := range worker.MapJob.outPuts {
			c.reducesToSchedule[k] = append(c.reducesToSchedule[k], v...)
		}
	} else {
		c.reduceFinishedCount++
	}
	c.mu.Unlock()
}

func (c *Coordinator) Schedule(s WorkerState) WorkerState {
	c.mu.Lock()
	if s.Id == 0 {
		s.Id = c.InitializeWorker()
	}
	worker := c.workers[s.Id]
	worker.JobStatus = s.JobStatus

	if worker.JobStatus == MapInProgress || worker.JobStatus == ReduceInProgress {
		return *worker
	}

	if len(c.files) != 0 {
		worker.Timestamp = time.Now()
		worker.JobStatus = MapSchedule
		worker.MapJob = MapJob{
			file:      c.files[0],
			nReducers: c.nReducers,
			outPuts:   make(map[int][]string),
		}
		c.mapJobs[c.files[0]] = c.workerId
		c.files = c.files[1:]
	}

	if len(c.mapJobs) == 0 && len(c.reducesToSchedule) != 0 {
		worker.Timestamp = time.Now()
		worker.JobStatus = ReduceSchedule
		worker.ReduceJob = ReduceJob{
			intermediateOutputs: c.reducesToSchedule[0],
		}
		c.reducesToSchedule = c.reducesToSchedule[1:]
	}
	c.mu.Unlock()
	return *worker
}

func Execute() {
	c := &Coordinator{
		files:             []string{"a", "b", "c", "d"},
		nReducers:         3,
		mapJobs:           make(map[string]int),
		reducesToSchedule: make([][]string, 3),
		workers:           make(map[int]*WorkerState),
		workerId:          0,
	}

	closes := make([]chan bool, 5)
	for i := 0; i < 5; i++ {
		go StartProcessing(c, closes[i])
	}

	for {
		// Server is working
	}
}
