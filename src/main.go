package main

import (
	"distributed_systems/crawler"
	"distributed_systems/heartbeat"
	"fmt"
)

func main() {
	heartbeat.Execute()
	crawler.Sequential("https://golang.org/", 4)
	fmt.Println("___________________________")
	crawler.Coordinator("https://golang.org/")
}
