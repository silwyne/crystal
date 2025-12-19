package source

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/Silwyne/crystal/api/operation/signal"
	"github.com/Silwyne/crystal/api/row"
)

type HttpSource struct {
	ctx          context.Context
	client       *http.Client
	url          string
	method       string
	headers      map[string]string
	deserializer HttpDeserializer
	pollInterval time.Duration
}

type HttpDeserializer func(*http.Response) (row.Row, error)

func NewHttpSource(url string, method string, headers map[string]string, timeout time.Duration, pollInterval time.Duration, deserializer HttpDeserializer) *HttpSource {
	source := HttpSource{
		ctx:          context.Background(),
		url:          url,
		method:       method,
		headers:      headers,
		deserializer: deserializer,
		client: &http.Client{
			Timeout: timeout,
		},
		pollInterval: pollInterval,
	}
	return &source
}

func (hs *HttpSource) Apply(source_channel chan row.Row, result_channel chan row.Row) signal.Signal {
	// Create HTTP request
	req, err := http.NewRequestWithContext(hs.ctx, hs.method, hs.url, nil)
	if err != nil {
		log.Printf("Error creating HTTP request: %v\n", err)
		return signal.FAILURE
	}

	// Add headers
	for key, value := range hs.headers {
		req.Header.Set(key, value)
	}

	// Send request
	resp, err := hs.client.Do(req)
	if err != nil {
		log.Printf("Error sending HTTP request: %v\n", err)
		return signal.FAILURE
	}
	defer resp.Body.Close()

	// Deserialize response
	deserializedRow, err := hs.deserializer(resp)
	if err != nil {
		log.Printf("Error deserializing HTTP response: %v\n", err)
		return signal.FAILURE
	}

	// Send row to result channel
	result_channel <- deserializedRow

	return signal.SUCCESS
}

func (k HttpSource) IsResultStateless() bool {
	return true
}

func (k HttpSource) GetName() string {
	return "HTTP_SOURCE"
}
