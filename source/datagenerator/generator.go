package datagenerator

import (
	"sync"

	"github.com/crystal/api/functions"
	"github.com/crystal/api/operation"
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

func (s DataGenerator) GetTransformationType() operation.TransformationType {
	return operation.SOURCE
}
