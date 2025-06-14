package functions

type MapFunction func(interface{}) (interface{}, bool)

type MapTransformation struct {
	Function MapFunction
}

func (m MapTransformation) Apply(data interface{}) (interface{}, bool) {
	result, boolResult := m.Function(data)
	return result, boolResult
}

func (m MapTransformation) GetResultStreamType() DataStreamType {
	return SingleOutputStream
}
