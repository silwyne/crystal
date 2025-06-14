package stack

import (
	"process-engine/pkg/container"
	"process-engine/pkg/functions"
	"sync"
)

type DataStack struct {
	parallelism int
}

func NewDataStack(parallelism int) DataStack {
	if parallelism < 1 {
		panic("parallelism can't be less than 1")
	}
	return DataStack{
		parallelism: parallelism,
	}
}

func (d DataStack) FromSource(source functions.SourceFunction) container.DataContainer {
	return container.DataContainer{}.AddTransformation(functions.SourceTransformation{Function: source})
}

func (d DataStack) Execute(container container.DataContainer) {

	transformations := container.Transformations

	if len(transformations) == 0 {
		panic("No transformations in pipeline")
	}
	if _, ok := transformations[0].(functions.SourceTransformation); !ok {
		panic("First transformation must be a source function")
	}

	d.runOperatorInParallel(transformations)
}

func (d DataStack) runOperatorInParallel(transformations []functions.Transformation) {
	var wg sync.WaitGroup
	var channel_holder []chan interface{}
	wg.Add(d.parallelism)
	for i := 0; i < d.parallelism; i++ {
		go executeTransformations(&wg, transformations, channel_holder)
	}
	wg.Wait()
}

func executeTransformations(
	wg *sync.WaitGroup,
	transformations []functions.Transformation,
	channel_holder []chan interface{}) {

	for id, transformation := range transformations {
		switch transformation.GetResultStreamType() {
		case functions.SourceStream:
			{
				source_channel := make(chan interface{})
				channel_holder = append(channel_holder, source_channel)
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						data, ok := transformation.Apply(nil)
						if !ok {
							break
						}
						source_channel <- data
					}
					close(source_channel)
				}()
			}

		case functions.MapStream:
			{
				source_channel := channel_holder[id-1]
				result_channel := make(chan interface{})
				channel_holder = append(channel_holder, result_channel)
				wg.Add(1)
				go func() {
					defer wg.Done()
					for input := range source_channel {
						data, ok := transformation.Apply(input)
						if ok {
							result_channel <- data
						}
					}
					close(result_channel)
				}()
			}
		case functions.SinkStream:
			{
				source_channel := channel_holder[id-1]
				wg.Add(1)
				go func() {
					defer wg.Done()
					for input := range source_channel {
						_, ok := transformation.Apply(input)
						if !ok {
							panic("error while sinking")
						}
					}
				}()
			}

		}
	}
}
