package main

import (
	"distributed_systems/heartbeat"
	"time"
)

func main() {
	close := make(chan bool)
	go heartbeat.Execute(close)

	<-time.After(time.Minute)
	close <- true
}
