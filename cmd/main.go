package main

import (
	"fmt"
	"process-engine/pkg/core"
	"time"
)

// region Defining Job
type JobContainer struct{}

func (j JobContainer) PollData() interface{} {
	mySimpleData := "source: " + time.Now().String()
	return mySimpleData
}

func (j JobContainer) Transform(input interface{}) interface{} {
	stringResult := input.(string) + ", transform: " + time.Now().String()
	return stringResult
}

func (j JobContainer) Sink(input interface{}) {
	fmt.Println(input.(string))
}

func main() {
	JobContainer := JobContainer{}
	jobRunner := core.NewJobRunner(JobContainer)
	jobRunner.Run()
}
