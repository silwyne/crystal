package ratelimiter

// RateLimiter defines the interface for rate limiting
type RateLimiter interface {
	Wait() bool   // Wait for next allowed operation
	IsDone() bool // Check if duration limit reached
	Start()       // Start timing
	Stop()        // Stop the limiter
}

type RateLimiterStatus int

const (
	RUNNING RateLimiterStatus = iota
	STOPPED
)
