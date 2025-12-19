package ratelimiter

import (
	"time"
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
	if ratePerSecond < 1 {
		panic("ratePerSecond can't be lower than 1")
	}
	if !infinite && durationSecond < 1 {
		panic("durationSecond can't be lower than 1 when your job is finite")
	}
	return &TokenBucketRateLimiter{
		ratePerSecond: ratePerSecond,
		durationMs:    durationSecond * 1000,
		infinite:      infinite,
		tokens:        ratePerSecond,
	}
}

func (r *TokenBucketRateLimiter) Start() {
	r.lastRefill = time.Now().UnixMilli()
	r.startTime = time.Now().UnixMilli()
	r.status = RUNNING
}

// Wait waits for the next allowed operation
func (r *TokenBucketRateLimiter) Wait() bool {
	if r.status == STOPPED {
		return false
	}

	// Check duration limit if not infinite
	if !r.infinite {
		now := time.Now().UnixMilli()
		if now-r.startTime >= r.durationMs {
			r.status = STOPPED
			return false
		}
	}

	// Try to use a token
	if r.tryTakeToken() {
		return true
	}

	// If no tokens, wait for refill
	return r.waitForToken()
}

// tryTakeToken attempts to take a token without waiting
func (r *TokenBucketRateLimiter) tryTakeToken() bool {
	now := time.Now().UnixMilli()

	// Refill tokens if needed
	r.refillTokens(now)

	// Take token if available
	if r.tokens > 0 {
		r.tokens--
		return true
	}

	return false
}

// waitForToken waits for a token to become available
func (r *TokenBucketRateLimiter) waitForToken() bool {
	now := time.Now().UnixMilli()

	// Calculate time to next refill
	timeSinceLastRefill := now - r.lastRefill
	timeToNextRefill := int64(1000) - timeSinceLastRefill

	if timeToNextRefill > 0 {
		time.Sleep(time.Duration(timeToNextRefill) * time.Millisecond)
	}

	// Refill tokens after waiting
	now = time.Now().UnixMilli()
	r.refillTokens(now)

	// Take a token
	r.tokens--
	return true
}

// refillTokens refills tokens based on elapsed time
func (r *TokenBucketRateLimiter) refillTokens(now int64) {
	elapsed := now - r.lastRefill
	if elapsed >= 1000 {
		refillCount := elapsed / 1000
		r.tokens = r.ratePerSecond
		r.lastRefill += refillCount * 1000
	}
}

// IsDone checks if the rate limiter has reached its duration limit
func (r *TokenBucketRateLimiter) IsDone() bool {
	if r.infinite {
		return false
	}

	now := time.Now().UnixMilli()
	return now-r.startTime >= r.durationMs
}

// Stop stops the rate limiter
func (r *TokenBucketRateLimiter) Stop() {
	r.status = STOPPED
}
