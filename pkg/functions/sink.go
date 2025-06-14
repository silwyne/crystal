package functions

type SinkFunction func(interface{}) bool

type SinkTransformation struct {
	Function SinkFunction
}

func (s SinkTransformation) Apply(data interface{}) (interface{}, bool) {
	resultBool := s.Function(data)
	return nil, resultBool
}

func (m SinkTransformation) GetResultStreamType() DataStreamType {
	return SinkStream
}
