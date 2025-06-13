package functions

type MapFunction func(interface{}) interface{}

type MapTransformation struct {
	Function MapFunction
}

func (m MapTransformation) Apply(data interface{}) (interface{}, bool) {
	return m.Function(data), true
}
