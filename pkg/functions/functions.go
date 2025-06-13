package functions

type AllJobInterfaces interface {
	SourceFunction
	MapFunction
	SinkFunction
}

type MapFunction interface {
	Map(input interface{}) interface{}
}

type SourceFunction interface {
	PollData() interface{}
}

type SinkFunction interface {
	Sink(input interface{})
}
