package functions

type AllJobInterfaces interface {
	SourceFunction
	TransformationFunction
	SinkFunction
}

type TransformationFunction interface {
	Transform(input interface{}) interface{}
}

type SourceFunction interface {
	PollData() interface{}
}

type SinkFunction interface {
	Sink(input interface{})
}
