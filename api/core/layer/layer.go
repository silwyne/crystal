package layer

import "github.com/crystal/api/operation"

type StreamRunnerLayer struct {
	operators []operation.Operator
}

func MakeStreamLayers(operators []operation.Operator) []StreamRunnerLayer {
	var layers []StreamRunnerLayer

	for id, operator := range operators {
		if id == 0 {
			// adding the first layer

			first_layer := StreamRunnerLayer{
				operators: []operation.Operator{operator},
			}
			layers = append(layers, first_layer)
			continue

		}

		// check if can chain with last operator
		last_operator := operators[id-1]
		if last_operator.GetChaining() &&
			(last_operator.GetParallelism() == operator.GetParallelism()) {
			// it can be chained so lets chain the operator and add to the current layer

			current_layer := &layers[len(layers)-1]
			current_layer.operators = append(current_layer.operators, operator)

		} else {
			// it can not be chained so lets make a new layer

			new_layer := StreamRunnerLayer{
				operators: []operation.Operator{operator},
			}

			layers = append(layers, new_layer)

		}
	}

	return layers
}
