package container

import "process-engine/api/functions"

type DataOperator struct {
	Transformer functions.Transformation
	Parallelism int
}

func (o *DataOperator) SetParallelism(parallalism int) {
	o.Parallelism = parallalism
}
