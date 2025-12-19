package httpc

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/Silwyne/crystal/api/operation/signal"
	"github.com/Silwyne/crystal/api/ratelimiter"
	"github.com/Silwyne/crystal/api/row"
)

type HttpSource struct {
	ctx          context.Context
	client       *http.Client
	url          string
	method       string
	headers      map[string]string
	deserializer HttpDeserializer
	rateLimiter  ratelimiter.RateLimiter
}

type HttpDeserializer func(*http.Response) (row.Row, error)

func NewHttpSource(url string, method string, headers map[string]string, timeout time.Duration, rateLimiter ratelimiter.RateLimiter, deserializer HttpDeserializer) *HttpSource {
	if rateLimiter == nil {
		panic("rateLimiter can't be nil")
	}

	if deserializer == nil {
		panic("deserializer can't be nil")
	}

	source := HttpSource{
		ctx:          context.Background(),
		url:          url,
		method:       method,
		headers:      headers,
		deserializer: deserializer,
		rateLimiter:  rateLimiter,
		client: &http.Client{
			Timeout: timeout,
		},
	}
	return &source
}

func (hs *HttpSource) Apply(source_channel chan row.Row, result_channel chan row.Row) signal.Signal {
	// Start the rate limiter
	hs.rateLimiter.Start()
	defer hs.rateLimiter.Stop()

	for {
		// Wait for rate limiter to allow next operation
		if !hs.rateLimiter.Wait() {
			// Rate limiter returns false when duration is reached or stopped
			return signal.SUCCESS
		}

		req, err := http.NewRequestWithContext(hs.ctx, hs.method, hs.url, nil)
		if err != nil {
			log.Printf("Error creating HTTP request: %v\n", err)
			return signal.FAILURE
		}

		// Add headers
		for key, value := range hs.headers {
			req.Header.Set(key, value)
		}

		resp, err := hs.client.Do(req)
		if err != nil {
			log.Printf("Error sending HTTP request: %v\n", err)
		}
		deserializedRow, err := hs.deserializer(resp)
		if err != nil {
			log.Printf("Error deserializing HTTP response: %v\n", err)
			return signal.FAILURE
		}
		result_channel <- deserializedRow
	}
}

func (k HttpSource) IsResultStateless() bool {
	return true
}

func (k HttpSource) GetName() string {
	return "HTTP_SOURCE"
}
