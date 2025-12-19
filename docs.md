# Crystal - Stream Processing Framework

Crystal is a Go-based stream processing framework that provides connectors for various data sources and sinks with built-in rate limiting capabilities.

## Table of Contents
- [HTTP Connector (`httpc`)](#http-connector-httpc)
- [Data Generator Connector (`datagenerator`)](#data-generator-connector-datagenerator)
- [Core Concepts](#core-concepts)
- [Quick Start](#quick-start)

## HTTP Connector (`httpc`)

The `httpc` connector allows you to consume data from HTTP endpoints with configurable rate limiting.

### Basic Usage

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "time"

    "github.com/Silwyne/crystal/api/core"
    "github.com/Silwyne/crystal/api/ratelimiter"
    "github.com/Silwyne/crystal/api/row"
    "github.com/Silwyne/crystal/connectors/httpc"
)

func main() {
    // Initialize stream processing environment
    env := core.NewStreamEnvironment()
    env.SetParallelism(1) // Set concurrency level

    // Define response deserializer
    deserializer := func(r *http.Response) (row.Row, error) {
        defer r.Body.Close()
        
        statusCode := r.StatusCode
        body, err := io.ReadAll(r.Body)
        if err != nil {
            return row.Row{}, fmt.Errorf("failed to read response body: %w", err)
        }

        // Create row with status code and response body
        return row.From(statusCode, string(body)), nil
    }

    // Configure rate limiter (1 request per 250ms, burst of 4)
    rl := ratelimiter.NewTokenBucketRateLimiter(
        1,     // rate per second
        4,     // burst capacity
        false, // don't block, return error when rate limited
    )

    // Create HTTP source
    source := httpc.NewHttpSource(
        "https://httpbin.org/get",           // URL
        "GET",                              // HTTP method
        map[string]string{},                // Headers
        500*time.Millisecond,               // Timeout
        rl,                                 // Rate limiter
        deserializer,                       // Response processor
    )

    // Build processing pipeline
    requests := env.FromSource(source)
    printingEvents := requests.Print()

    // Execute the stream
    env.Execute(printingEvents)
}
```

### Configuration Options

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| URL | `string` | Target endpoint URL | Required |
| Method | `string` | HTTP method (GET, POST, etc.) | Required |
| Headers | `map[string]string` | HTTP headers | `nil` |
| Timeout | `time.Duration` | Request timeout | Required |
| RateLimiter | `ratelimiter.RateLimiter` | Controls request frequency | Required |
| Deserializer | `func(*http.Response) (row.Row, error)` | Response processor | Required |

## Data Generator Connector (`datagenerator`)

The `datagenerator` connector produces synthetic data streams at a controlled rate.

### Basic Usage

```go
package main

import (
    "github.com/Silwyne/crystal/api/core"
    "github.com/Silwyne/crystal/api/ratelimiter"
    "github.com/Silwyne/crystal/api/row"
    "github.com/Silwyne/crystal/connectors/datagenerator"
)

func main() {
    // Initialize environment
    env := core.NewStreamEnvironment()
    env.SetParallelism(1)

    // Configure rate limiter (1 event per 200ms, burst of 5)
    rl := ratelimiter.NewTokenBucketRateLimiter(1, 5, false)

    // Create data generator with custom generator function
    source := datagenerator.NewDataGenerator(
        rl,
        func() row.Row {
            return row.From("hi") // Generate rows containing "hi"
        },
    )

    // Build and execute pipeline
    ds := env.FromSource(source)
    sinkDs := ds.Print()
    env.Execute(sinkDs)
}
```

### Custom Generator Functions

You can create more complex data generators:

```go
package main

import (
    "fmt"
    "time"

    "github.com/Silwyne/crystal/api/row"
)

func timestampedDataGenerator() func() row.Row {
    counter := 0
    return func() row.Row {
        counter++
        return row.From(
            counter,
            time.Now().Unix(),
            fmt.Sprintf("event-%d", counter),
        )
    }
}

// Usage in NewDataGenerator:
// source := datagenerator.NewDataGenerator(rl, timestampedDataGenerator())
```

## Core Concepts

### Rate Limiting

Crystal provides a token bucket rate limiter with the following configuration:

```go
// NewTokenBucketRateLimiter(rate, burst, block)
rl := ratelimiter.NewTokenBucketRateLimiter(10, 20, true)

// Parameters:
// - rate: requests per second
// - burst: maximum burst capacity
// - block: whether to block when rate limited (true) or return error (false)
```

### Rows

Rows are the fundamental data unit in Crystal, created using `row.From()`:

```go
// Single value row
row1 := row.From("value")

// Multiple values row
row2 := row.From(1, "text", true, 3.14)

// Structured data row
row3 := row.From(map[string]interface{}{
    "id":   123,
    "name": "example",
})
```

### Stream Environment

The stream environment manages the execution context:

```go
env := core.NewStreamEnvironment()

// Configuration options
env.SetParallelism(4)    // Number of parallel workers
env.SetBufferSize(1000)  // Buffer size for streaming
```

## Quick Start

1. **Install Crystal**:
```bash
go get github.com/Silwyne/crystal
```

2. **Create a simple HTTP consumer**:
```go
package main

import (
    "github.com/Silwyne/crystal/api/core"
    "github.com/Silwyne/crystal/connectors/httpc"
)

func main() {
    env := core.NewStreamEnvironment()
    
    // Simple HTTP source without deserialization
    source := httpc.NewSimpleHttpSource(
        "https://api.example.com/data",
        "GET",
        nil,
        5*time.Second,
    )
    
    env.FromSource(source).Print().Execute()
}
```

3. **Run your application**:
```bash
go run main.go
```

## Best Practices

1. **Rate Limiting**: Always use rate limiters when calling external APIs to avoid being blocked
2. **Error Handling**: Implement proper error handling in deserializer functions
3. **Resource Management**: Set appropriate timeouts for HTTP requests
4. **Concurrency**: Adjust parallelism based on your system capabilities and workload

## Troubleshooting

### Common Issues

1. **Rate limiting errors**: Increase burst capacity or reduce request rate
2. **Timeout errors**: Adjust the timeout duration based on API response times
3. **Memory issues**: Reduce buffer size or implement backpressure strategies
