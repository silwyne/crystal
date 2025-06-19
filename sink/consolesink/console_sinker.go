package consolesink

import (
	"fmt"
	"process-engine/api/functions"
	"process-engine/api/transformation"
	"sync"
)

type ConsoleSink struct{}

func NewConsoleSinker() ConsoleSink {
	return ConsoleSink{}
}

func consoleSinkFunction(input interface{}) bool {
	_, err := fmt.Println(input.(string))
	return err == nil
}

func (cs ConsoleSink) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	st := functions.SinkTransformation{
		Function: consoleSinkFunction,
	}
	return st.ExecuteTransformation(wg, source_channel)
}

func (cs ConsoleSink) GetTransformationType() transformation.TransformationType {
	return transformation.SINK
}
