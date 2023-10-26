package main

import (
	"distributed_systems/heartbeat"
	"time"
)

func main() {
	go heartbeat.Execute()

	<-time.After(time.Hour)
}
