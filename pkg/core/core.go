package core

import (
	"process-engine/pkg/functions"
	"time"
)

type JobRunner struct {
	sourceFunction    functions.SourceFunction
	transformFunction functions.TransformationFunction
	sinkFunction      functions.SinkFunction
}

func NewJobRunner(input functions.AllJobInterfaces) JobRunner {
	return JobRunner{
		sourceFunction:    input,
		transformFunction: input,
		sinkFunction:      input,
	}
}

func (j JobRunner) Run() {
	for {
		data := j.sourceFunction.PollData()
		transformed := j.transformFunction.Transform(data)
		j.sinkFunction.Sink(transformed)

		// simulation
		time.Sleep(time.Second)
	}
}
