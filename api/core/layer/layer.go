package layer

import (
	"errors"
	"sync"

	"github.com/crystal/api/operation"
	"github.com/crystal/api/row"
)

/*
TODO: Implement TaskSlot run function
Also change the operation.Transformation in a way that makes chaining possible
If you don't this Struct would be meaningless
*/
type StreamRunnerLayer struct {
	slot        TaskSlot
	parallelism int
}

type TaskSlot struct {
	operators []operation.Operator
}

func MakeStreamLayers(operators []operation.Operator) []StreamRunnerLayer {
	var layers []StreamRunnerLayer

	for id, operator := range operators {
		if id == 0 {
			// adding the first layer

			first_layer := StreamRunnerLayer{
				slot: TaskSlot{
					operators: []operation.Operator{operator},
				},
				parallelism: operator.GetParallelism(),
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
			current_layer.slot.operators = append(current_layer.slot.operators, operator)

		} else {
			// it can not be chained so lets make a new layer

			new_layer := StreamRunnerLayer{
				slot: TaskSlot{
					operators: []operation.Operator{operator},
				},
				parallelism: operator.GetParallelism(),
			}

			layers = append(layers, new_layer)

		}
	}

	return layers
}

func (srl *StreamRunnerLayer) Run(wg *sync.WaitGroup, source_channels []chan row.Row, result_channels []chan row.Row) error {
	var slot *TaskSlot = &srl.slot
	if len(source_channels) != len(slot.operators) {
		err := errors.New("number of source channels doesn't match the number of operators")
		return err
	}
	//TODO: Implement logic
	return nil
}
