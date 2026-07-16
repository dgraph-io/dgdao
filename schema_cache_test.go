/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/stretchr/testify/require"

	dg "github.com/dgraph-io/dgdao"
)

type cachedNode struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty" dgraph:"CachedNode"`
	Name  string   `json:"cachedNodeName,omitempty" dgraph:"index=exact"`
}

// dropAllRaw drops all data and schema via a raw Alter on the underlying dgo
// client, bypassing Client.DropAll so the client's schema cache is NOT
// invalidated. Used to observe whether a write re-checks the schema.
func dropAllRaw(t *testing.T, client dg.Client) {
	t.Helper()
	raw, cleanup, err := client.DgraphClient()
	require.NoError(t, err)
	defer cleanup()
	require.NoError(t, raw.Alter(context.Background(), &api.Operation{DropAll: true}))
}

// TestSchemaCache_SkipsValidationAfterFirstWrite proves the cache actually
// short-circuits the per-write schema fetch: after one successful insert, the
// schema is dropped out-of-band (bypassing DropAll's invalidation), and a
// second insert must NOT fail with "schema validation failed" — the check was
// served from cache rather than re-fetched.
func TestSchemaCache_SkipsValidationAfterFirstWrite(t *testing.T) {
	client, err := dg.NewClient("file://" + GetTempDir(t)) // schema cache on by default
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	require.NoError(t, client.UpdateSchema(ctx, &cachedNode{}))
	require.NoError(t, client.Insert(ctx, &cachedNode{DType: []string{"CachedNode"}, Name: "warm"}))

	dropAllRaw(t, client)

	err = client.Insert(ctx, &cachedNode{DType: []string{"CachedNode"}, Name: "cached"})
	if err != nil {
		require.NotContains(t, err.Error(), "schema validation failed",
			"cached type must skip the schema round-trip")
	}
}

// TestSchemaCache_Disabled proves WithSchemaCache(false) preserves the
// original per-write behavior: the same out-of-band schema drop makes the
// very next insert fail validation.
func TestSchemaCache_Disabled(t *testing.T) {
	client, err := dg.NewClient("file://"+GetTempDir(t), dg.WithSchemaCache(false))
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	require.NoError(t, client.UpdateSchema(ctx, &cachedNode{}))
	require.NoError(t, client.Insert(ctx, &cachedNode{DType: []string{"CachedNode"}, Name: "warm"}))

	dropAllRaw(t, client)

	err = client.Insert(ctx, &cachedNode{DType: []string{"CachedNode"}, Name: "unchecked"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema validation failed")
}

// TestSchemaCache_DropAllInvalidates verifies that Client.DropAll flushes the
// cache: a type that validated before the drop must fail validation after it.
func TestSchemaCache_DropAllInvalidates(t *testing.T) {
	client, err := dg.NewClient("file://" + GetTempDir(t))
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	require.NoError(t, client.UpdateSchema(ctx, &cachedNode{}))
	require.NoError(t, client.Insert(ctx, &cachedNode{DType: []string{"CachedNode"}, Name: "warm"}))

	require.NoError(t, client.DropAll(ctx))

	err = client.Insert(ctx, &cachedNode{DType: []string{"CachedNode"}, Name: "stale"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema validation failed")
}

// TestSchemaCache_AutoSchema exercises the AutoSchema path with the cache on:
// repeated writes of the same type must keep working (the schema sync runs
// once and is skipped thereafter).
func TestSchemaCache_AutoSchema(t *testing.T) {
	client, err := dg.NewClient("file://"+GetTempDir(t), dg.WithAutoSchema(true))
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		require.NoError(t, client.Insert(ctx, &cachedNode{DType: []string{"CachedNode"}, Name: fmt.Sprintf("row-%d", i)}))
	}
}
