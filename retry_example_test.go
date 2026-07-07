/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"time"

	"github.com/dgraph-io/dgdao"
)

// ExampleClient_WithRetry wraps a mutation so an aborted transaction — the
// error Dgraph returns when concurrent writers conflict on an indexed
// predicate — is retried with exponential backoff instead of surfacing to the
// caller. Non-abort errors return immediately; the context bounds the total
// wait.
func ExampleClient_WithRetry() {
	client, _ := dgdao.NewClient("dgraph://localhost:9080")
	defer client.Close()

	ctx := context.Background()
	entity := &RetryEntity{Name: "alice", Value: 1}

	err := client.WithRetry(ctx, dgdao.DefaultRetryPolicy, func() error {
		return client.Insert(ctx, entity)
	})
	if err != nil {
		panic(err)
	}
}

// ExampleRetryPolicy shows a custom backoff schedule: three retries, 50ms base
// delay doubling each attempt, capped at 2s, with 20% jitter to spread
// concurrent retriers apart.
func ExampleRetryPolicy() {
	client, _ := dgdao.NewClient("dgraph://localhost:9080")
	defer client.Close()

	policy := dgdao.RetryPolicy{
		MaxRetries: 3,
		BaseDelay:  50 * time.Millisecond,
		MaxDelay:   2 * time.Second,
		Jitter:     0.2,
	}

	ctx := context.Background()
	_ = client.WithRetry(ctx, policy, func() error {
		return client.Insert(ctx, &RetryEntity{Name: "bob", Value: 2})
	})
}
