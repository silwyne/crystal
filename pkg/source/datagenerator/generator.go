package datagenerator

import (
	"process-engine/internals/functions"
	"sync"
)

type DataGeneratorTransformation struct {
	dataGenFunction functions.SourceFunction
}

func NewDataGenerator(dataGenFunction functions.SourceFunction) *DataGeneratorTransformation {
	return &DataGeneratorTransformation{dataGenFunction: dataGenFunction}
}

func (s DataGeneratorTransformation) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	st := functions.SourceTransformation{
		Function: s.dataGenFunction,
	}
	return st.ExecuteTransformation(wg, source_channel)
}
