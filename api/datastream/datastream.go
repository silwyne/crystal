package datastream

import (
	"fmt"
	"log"

	"github.com/crystal/api/configuration"
	"github.com/crystal/api/functions"
	"github.com/crystal/api/operation"
)

type DataStream[T any] struct {
	configs   configuration.StreamConfig
	Operators []operation.Operator
}

func Map[IN any, OUT any](ds *DataStream[IN], mapper functions.MapTransformation[IN, OUT]) *DataStream[OUT] {
	result := AddTransformation(ds, mapper)
	return result
}

func FlatMap[IN any, OUT any](ds *DataStream[IN], flatmapper functions.FlatMapTransformation[IN, OUT]) *DataStream[OUT] {
	result := AddTransformation(ds, flatmapper)
	return result
}

func Sink[IN any](ds *DataStream[IN], sinker functions.SinkTransformation[IN, any]) *DataStream[any] {
	result := AddTransformation(ds, sinker)
	return result
}

func AddTransformation[IN any, OUT any](ds *DataStream[IN], transformer operation.Transformation[IN, OUT]) *DataStream[OUT] {

	untyped_transformation := transformer.(operation.Transformation[any, any])
	operator := operation.NewOperator(untyped_transformation, ds.configs.GlobalParallelism)

	return &DataStream[OUT]{
		Operators: append(ds.Operators, operator),
		configs:   ds.configs,
	}
}

func (ds *DataStream[IN]) SetParallelism(parallelism int) *DataStream[IN] {
	last_operator := &ds.Operators[len(ds.Operators)-1]
	last_operator.SetParallelism(parallelism)
	return ds
}

func (ds *DataStream[IN]) SetConfigs(configs configuration.StreamConfig) {
	ds.configs = configs
}

func (ds *DataStream[IN]) Print() *DataStream[any] {
	sinker := functions.SinkTransformation[IN, any]{
		SinkFunction: func(i IN) {
			fmt.Println("> " + fmt.Sprint(i))
		},
	}
	stream := Sink(ds, sinker)
	return stream
}

func (ds *DataStream[IN]) PrintDetails() {
	for id, operator := range ds.Operators {
		log.Printf("n.%v name: %v, parallelism: %v\n", id, operator.Transformer.GetName(), operator.Parallelism)
	}
}
