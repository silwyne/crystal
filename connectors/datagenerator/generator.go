package datagenerator

import (
	"github.com/Silwyne/crystal/api/operation/signal"
	"github.com/Silwyne/crystal/api/ratelimiter"
	"github.com/Silwyne/crystal/api/row"
)

type DataGenerator struct {
	infinite       bool
	ratePerSecond  int
	durationSecond int
	generator      Generator
	rateLimiter    ratelimiter.RateLimiter
}

type Generator func() row.Row

func NewDataGenerator(infinite bool, ratePerSecond int, durationSecond int, generator Generator) *DataGenerator {
	if ratePerSecond < 1 {
		panic("ratePerSecond can't be lower than 1")
	}
	if !infinite && durationSecond < 1 {
		panic("durationSecond can't be lower than 1")
	}
	if generator == nil {
		panic("generator can't be nil")
	}

	// Create rate limiter instance
	rl := ratelimiter.NewTokenBucketRateLimiter(
		int64(ratePerSecond),
		int64(durationSecond),
		infinite,
	)

	return &DataGenerator{
		infinite:       infinite,
		ratePerSecond:  ratePerSecond,
		durationSecond: durationSecond,
		generator:      generator,
		rateLimiter:    rl,
	}
}

func (dg *DataGenerator) Apply(source_channel chan row.Row, result_channel chan row.Row) signal.Signal {
	// Start the rate limiter
	dg.rateLimiter.Start()
	defer dg.rateLimiter.Stop()

	for {
		// Wait for rate limiter to allow next operation
		if !dg.rateLimiter.Wait() {
			// Rate limiter returns false when duration is reached or stopped
			return signal.SUCCESS
		}

		// Generate and send data
		generated_row := dg.generator()
		result_channel <- generated_row
	}
}

func (dg DataGenerator) IsResultStateless() bool {
	return true
}

func (dg DataGenerator) GetName() string {
	return "DATA_GENERATOR_SOURCE"
}
