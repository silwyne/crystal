package consolesink

import (
	"fmt"
	"process-engine/api/functions"
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

func (s ConsoleSink) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	st := functions.SinkTransformation{
		Function: consoleSinkFunction,
	}
	return st.ExecuteTransformation(wg, source_channel)
}

func (s ConsoleSink) GetName() string {
	return "ConsoleSink"
}
