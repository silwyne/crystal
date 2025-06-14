package functions

type DataStreamType string

const (
	SourceStream       DataStreamType = "SourceStream"
	SingleOutputStream DataStreamType = "SingleOutputStream"
	SinkStream         DataStreamType = "SinkStream"
)

type Transformation interface {
	Apply(data interface{}) (interface{}, bool)
	GetResultStreamType() DataStreamType
}
