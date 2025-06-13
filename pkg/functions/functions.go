package functions

type Transformation interface {
	Apply(data interface{}) (interface{}, bool)
}
