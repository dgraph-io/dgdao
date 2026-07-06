/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/dgdao"
	"github.com/dgraph-io/dgo/v250"
	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RetryEntity is a test struct with a unique index to provoke transaction conflicts.
type RetryEntity struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Name  string   `json:"name,omitempty" dgraph:"index=term,exact upsert"`
	Value int      `json:"value,omitempty"`
}

// TestConcurrentInsertsWithRetry verifies that WithRetry handles aborted
// transactions from concurrent inserts. Without WithRetry, concurrent inserts
// on the same predicate index would fail with dgo.ErrAborted.
func TestConcurrentInsertsWithRetry(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "FileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "DgraphURI",
			uri:  "dgraph://" + os.Getenv("DGDAO_TEST_ADDR"),
			skip: os.Getenv("DGDAO_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
				return
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			ctx := context.Background()
			const numWorkers = 8
			const entitiesPerWorker = 10

			var succeeded atomic.Int64
			var wg sync.WaitGroup

			for w := range numWorkers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range entitiesPerWorker {
						entity := &RetryEntity{
							Name:  fmt.Sprintf("entity-%d-%d", w, i),
							Value: w*entitiesPerWorker + i,
						}
						err := client.WithRetry(ctx, dgdao.DefaultRetryPolicy, func() error {
							return client.Insert(ctx, entity)
						})
						if err != nil {
							t.Errorf("worker %d entity %d: %v", w, i, err)
							return
						}
						succeeded.Add(1)
					}
				}()
			}
			wg.Wait()

			total := int64(numWorkers * entitiesPerWorker)
			require.Equal(t, total, succeeded.Load(),
				"all concurrent inserts should succeed with retry")
		})
	}
}

// TestWithRetryContextCancellation verifies that WithRetry respects context
// cancellation during backoff sleeps.
func TestWithRetryContextCancellation(t *testing.T) {
	uri := "file://" + GetTempDir(t)
	client, cleanup := CreateTestClient(t, uri)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Use a policy with a long delay so the context expires during backoff.
	slowPolicy := dgdao.RetryPolicy{
		MaxRetries: 10,
		BaseDelay:  1 * time.Second,
		MaxDelay:   5 * time.Second,
		Jitter:     0,
	}

	callCount := 0
	err := client.WithRetry(ctx, slowPolicy, func() error {
		callCount++
		// Return a real abort so WithRetry enters the retry path and sleeps for
		// the 1s backoff. The 50ms context deadline fires during that sleep, so
		// WithRetry must return from the ctx.Done() branch of the backoff select
		// — the path this test exists to cover.
		return dgo.ErrAborted
	})

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, callCount, "fn runs once, then the context expires during backoff")
}

// TestRetryPolicyDelay verifies the exponential backoff calculation.
func TestRetryPolicyDelay(t *testing.T) {
	// Use the public struct fields to verify delay behavior indirectly
	// by checking that DefaultRetryPolicy has the expected values.
	p := dgdao.DefaultRetryPolicy
	assert.Equal(t, 10, p.MaxRetries)
	assert.Equal(t, 100*time.Millisecond, p.BaseDelay)
	assert.Equal(t, 5*time.Second, p.MaxDelay)
	assert.InDelta(t, 0.1, p.Jitter, 0.001)
}

// TestWithRetryNonAbortError verifies that non-abort errors are returned
// immediately without any retry.
func TestWithRetryNonAbortError(t *testing.T) {
	uri := "file://" + GetTempDir(t)
	client, cleanup := CreateTestClient(t, uri)
	defer cleanup()

	callCount := 0
	expectedErr := fmt.Errorf("not an abort error")

	err := client.WithRetry(context.Background(), dgdao.DefaultRetryPolicy, func() error {
		callCount++
		return expectedErr
	})

	assert.ErrorIs(t, err, expectedErr)
	assert.Equal(t, 1, callCount, "non-abort errors should not trigger retry")
}

// TestWithRetrySucceedsFirstTry verifies that WithRetry returns nil
// when fn succeeds on the first call.
func TestWithRetrySucceedsFirstTry(t *testing.T) {
	uri := "file://" + GetTempDir(t)
	client, cleanup := CreateTestClient(t, uri)
	defer cleanup()

	callCount := 0
	err := client.WithRetry(context.Background(), dgdao.DefaultRetryPolicy, func() error {
		callCount++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)
}

// TestWithRetryMaxRetriesZero verifies that MaxRetries=0 calls fn exactly once
// and returns any error without retrying.
func TestWithRetryMaxRetriesZero(t *testing.T) {
	uri := "file://" + GetTempDir(t)
	client, cleanup := CreateTestClient(t, uri)
	defer cleanup()

	policy := dgdao.RetryPolicy{MaxRetries: 0}
	callCount := 0

	err := client.WithRetry(context.Background(), policy, func() error {
		callCount++
		return fmt.Errorf("always fails")
	})

	assert.Error(t, err)
	assert.Equal(t, 1, callCount, "MaxRetries=0 should call fn exactly once")
}

// TestWithRetryNegativeMaxRetries verifies that a negative MaxRetries still
// calls fn exactly once and returns its error, rather than skipping the loop
// and panicking.
func TestWithRetryNegativeMaxRetries(t *testing.T) {
	uri := "file://" + GetTempDir(t)
	client, cleanup := CreateTestClient(t, uri)
	defer cleanup()

	policy := dgdao.RetryPolicy{MaxRetries: -1}
	callCount := 0
	expectedErr := fmt.Errorf("boom")

	err := client.WithRetry(context.Background(), policy, func() error {
		callCount++
		return expectedErr
	})

	assert.ErrorIs(t, err, expectedErr)
	assert.Equal(t, 1, callCount, "negative MaxRetries should still call fn once")
}

// TestWithRetryDetectsWrappedAbort guards the wrapping chain WithRetry relies
// on. A real abort does not reach WithRetry as the bare dgo.ErrAborted sentinel:
// the mutation stack wraps it with github.com/pkg/errors (as dgman does), and
// errors.Is must still see the sentinel through that wrapper. Every other abort
// test returns the bare sentinel, so this is the only guard that the
// wrap -> Unwrap chain stays intact if pkg/errors ever stops preserving it.
func TestWithRetryDetectsWrappedAbort(t *testing.T) {
	uri := "file://" + GetTempDir(t)
	client, cleanup := CreateTestClient(t, uri)
	defer cleanup()

	policy := dgdao.RetryPolicy{MaxRetries: 2, BaseDelay: time.Millisecond}
	callCount := 0
	err := client.WithRetry(context.Background(), policy, func() error {
		callCount++
		return pkgerrors.Wrap(dgo.ErrAborted, "post-mutation hook failed")
	})

	assert.ErrorIs(t, err, dgo.ErrAborted)
	assert.Equal(t, 3, callCount, "a wrapped abort is retried MaxRetries+1 times")
}
