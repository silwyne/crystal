package functions

import "sync"

type DataStreamType string

const (
	SourceStream DataStreamType = "SourceStream"
	MapStream    DataStreamType = "MapStream"
	SinkStream   DataStreamType = "SinkStream"
)

type Transformation interface {
	Apply(data interface{}) (interface{}, bool)
	GetResultStreamType() DataStreamType
	ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{}
}
