package ratelimiter

import (
	"time"
)

// RateLimiter defines the interface for rate limiting
type RateLimiter interface {
	Allow() bool
	Stop()
}

type RateLimiterStatus int

const (
	RUNNING RateLimiterStatus = iota
	STOPPED
)

// TokenBucketRateLimiter implements a token bucket rate limiter
type TokenBucketRateLimiter struct {
	ratePerSecond int64
	durationMs    int64
	infinite      bool

	tokens     int64
	lastRefill int64
	startTime  int64
	status     RateLimiterStatus
}

// NewTokenBucketRateLimiter creates a new rate limiter
func NewTokenBucketRateLimiter(ratePerSecond int64, durationSecond int64, infinite bool) *TokenBucketRateLimiter {
	r := &TokenBucketRateLimiter{
		ratePerSecond: ratePerSecond,
		durationMs:    durationSecond * 1000,
		infinite:      infinite,
		tokens:        ratePerSecond,
		lastRefill:    time.Now().UnixMilli(),
		startTime:     time.Now().UnixMilli(),
		status:        RUNNING,
	}

	return r
}

// Allow checks if a request can be made according to the rate limit
func (r *TokenBucketRateLimiter) Allow() bool {
	now := time.Now().UnixMilli()

	// Check duration limit if not infinite
	if !r.infinite {
		if now-r.startTime >= r.durationMs {
			r.status = STOPPED
			return false
		}
	}

	// Refill tokens based on elapsed time
	elapsed := now - r.lastRefill
	if elapsed >= 1000 {
		refillCount := elapsed / 1000
		r.tokens = r.ratePerSecond
		r.lastRefill += refillCount * 1000
	}

	// Check if tokens are available
	if r.tokens > 0 {
		r.tokens--
		return true
	}

	// Wait for next second if no tokens
	timeToNextRefill := 1000 - (now - r.lastRefill)
	if timeToNextRefill > 0 {
		time.Sleep(time.Duration(timeToNextRefill) * time.Millisecond)
	}

	// Reset for new second
	r.tokens = r.ratePerSecond - 1
	r.lastRefill = time.Now().UnixMilli()

	return true
}
