package functions

type SourceFunction func() (interface{}, bool)

type SourceTransformation struct {
	Function SourceFunction
}

func (s SourceTransformation) Apply(data interface{}) (interface{}, bool) {
	result, boolResult := s.Function()
	return result, boolResult
}

func (m SourceTransformation) GetResultStreamType() DataStreamType {
	return SourceStream
}
