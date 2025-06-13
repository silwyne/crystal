package functions

type SourceFunction func() (interface{}, bool)

type MapFunction func(interface{}) interface{}

type SinkFunction func(interface{})
