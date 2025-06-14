package functions

type DataStreamType string

const (
	SourceStream DataStreamType = "SourceStream"
	MapStream    DataStreamType = "MapStream"
	SinkStream   DataStreamType = "SinkStream"
)

type Transformation interface {
	Apply(data interface{}) (interface{}, bool)
	GetResultStreamType() DataStreamType
}
