/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/dgraph-io/dgo/v250"
)

// RetryPolicy controls how WithRetry handles aborted transactions.
// Modeled after dgraph4j's RetryPolicy: exponential backoff with jitter.
type RetryPolicy struct {
	// MaxRetries is the maximum number of retry attempts after the initial try.
	MaxRetries int

	// BaseDelay is the initial delay before the first retry.
	// Subsequent delays grow exponentially: BaseDelay * 2^attempt.
	BaseDelay time.Duration

	// MaxDelay caps the backoff duration. No single delay exceeds this.
	MaxDelay time.Duration

	// Jitter adds randomness to each delay to prevent thundering herd.
	// Expressed as a fraction of the computed delay (e.g. 0.1 = 10%).
	Jitter float64
}

// DefaultRetryPolicy mirrors dgraph4j's defaults:
// 10 retries, 100ms base delay, 5s max delay, 10% jitter.
var DefaultRetryPolicy = RetryPolicy{
	MaxRetries: 10,
	BaseDelay:  100 * time.Millisecond,
	MaxDelay:   5 * time.Second,
	Jitter:     0.1,
}

// delay computes the backoff duration for a given attempt (0-indexed):
// the exponential BaseDelay*2^attempt, plus up to Jitter of itself, clamped
// so the result never exceeds MaxDelay. Clamping last keeps the documented
// invariant that no single delay exceeds MaxDelay — adding jitter after the
// cap would let the delay overshoot it.
func (p RetryPolicy) delay(attempt int) time.Duration {
	// Cap the exponential before jitter so a large attempt cannot overflow the
	// shift. attempt comes from a range loop (>= 0) and is bounded below 63;
	// exp <= 0 means the shift overflowed anyway, which we treat as the cap.
	d := p.MaxDelay
	if attempt >= 0 && attempt < 63 {
		if exp := p.BaseDelay << attempt; exp > 0 && exp < p.MaxDelay {
			d = exp
		}
	}
	if p.Jitter > 0 {
		// Backoff jitter spreads retriers apart; it does not need a
		// cryptographic RNG, so math/rand/v2 is appropriate here.
		d += time.Duration(float64(d) * p.Jitter * rand.Float64()) //nolint:gosec // G404: jitter is not security-sensitive
		if d > p.MaxDelay {
			d = p.MaxDelay
		}
	}
	return d
}

// WithRetry executes fn, retrying on aborted transactions according to policy.
//
// This is an opt-in mechanism modeled after dgraph4j's client.withRetry().
// The caller wraps their mutation logic in fn; WithRetry handles creating
// fresh attempts with exponential backoff when Dgraph returns a transaction
// abort due to concurrent conflicts.
//
// fn is called at least once. On each aborted-transaction error, WithRetry
// waits according to the policy's backoff schedule and calls fn again, up to
// policy.MaxRetries additional times. Non-abort errors are returned immediately.
//
// The context is checked between retries; if cancelled during a backoff sleep,
// the context error is returned.
//
// Usage:
//
//	err := client.WithRetry(ctx, dgdao.DefaultRetryPolicy, func() error {
//	    return client.Insert(ctx, &entity)
//	})
func (c client) WithRetry(ctx context.Context, policy RetryPolicy, fn func() error) error {
	// A negative MaxRetries would make the loop run zero times and never call
	// fn; clamp to zero so fn always runs at least once, as documented.
	maxRetries := policy.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	for attempt := range maxRetries + 1 {
		err := fn()
		if err == nil {
			return nil
		}
		if !errors.Is(err, dgo.ErrAborted) || attempt >= maxRetries {
			return err
		}
		d := policy.delay(attempt)
		c.logger.V(1).Info("Transaction aborted, retrying",
			"attempt", attempt+1, "maxRetries", maxRetries, "delay", d)
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Unreachable: the loop runs at least once and returns on every path.
	panic("unreachable")
}
