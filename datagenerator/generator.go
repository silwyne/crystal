package datagenerator

import (
	"time"

	"github.com/crystal/api/row"
)

type DataGenerator struct {
	infinite       bool
	ratePerSecond  int
	durationSecond int
	generator      Generator
}

type Generator func() row.Row

func NewDataGenerator(infinite bool, ratePerSecond int, durationSecond int, generator Generator) *DataGenerator {
	if ratePerSecond < 1 {
		panic("ratePerSecond can't be lower than 1")
	}
	if durationSecond < 1 {
		panic("durationSecond can't be lower than 1")
	}
	if generator == nil {
		panic("generator can't be nil")
	}
	return &DataGenerator{
		infinite:       infinite,
		ratePerSecond:  ratePerSecond,
		durationSecond: durationSecond,
	}
}

func (dg *DataGenerator) Apply(source_channel chan row.Row, result_channel chan row.Row) {
	if dg.infinite {
		dg.GenerateForEver(result_channel)
	} else {
		dg.GenerateAndDie(result_channel)
	}
}

func (dg *DataGenerator) GenerateForEver(result_channel chan row.Row) {
	var round_left int = dg.ratePerSecond
	var round_start_time int = int(time.Now().UnixMilli())
	for {
		generated_row := dg.generator()
		result_channel <- generated_row
		round_left--
		if round_left <= 0 {
			now_time := int(time.Now().UnixMilli())
			time_passed := now_time - round_start_time
			time_left := 1000 - time_passed
			if time_left > 0 {
				time.Sleep(time.Duration(time.Duration(time_left).Milliseconds()))
			}
			round_start_time = int(time.Now().UnixMilli())
			round_left = dg.ratePerSecond
		}
	}
}

func (dg *DataGenerator) GenerateAndDie(result_channel chan row.Row) {
	var base_start_time int = int(time.Now().UnixMilli())
	var duration_millisecond int = dg.durationSecond * 1000
	var round_left int = dg.ratePerSecond
	var round_start_time int = int(time.Now().UnixMilli())
	for {
		generated_row := dg.generator()
		result_channel <- generated_row
		round_left--
		if round_left <= 0 {
			now_time := int(time.Now().UnixMilli())
			time_passed := now_time - round_start_time
			time_left := 1000 - time_passed
			if time_left > 0 {
				time.Sleep(time.Duration(time.Duration(time_left).Milliseconds()))
			}

			total_time_passed := base_start_time - now_time
			if total_time_passed >= duration_millisecond {
				return
			}

			round_start_time = int(time.Now().UnixMilli())
			round_left = dg.ratePerSecond
		}
	}
}

func (dg DataGenerator) IsResultStateless() bool {
	return true
}

func (dg DataGenerator) GetName() string {
	return "DATA_GENERATOR_SOURCE"
}
