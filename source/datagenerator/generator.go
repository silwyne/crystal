package datagenerator

import (
	"process-engine/api/functions"
	"process-engine/api/transformation"
	"sync"
)

type DataGenerator struct {
	dataGenFunction functions.SourceFunction
}

func NewDataGenerator(dataGenFunction functions.SourceFunction) *DataGenerator {
	return &DataGenerator{dataGenFunction: dataGenFunction}
}

func (dg DataGenerator) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	st := functions.SourceTransformation{
		Function: dg.dataGenFunction,
	}
	return st.ExecuteTransformation(wg, source_channel)
}

func (s DataGenerator) GetTransformationType() transformation.TransformationType {
	return transformation.SOURCE
}
