/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"testing"

	dg "github.com/dgraph-io/dgdao"
	"github.com/stretchr/testify/require"
)

// studioRecord is a schema-defining record (implements dg.Schema). studioWrapper
// wraps it and exposes Record, exactly as a dgdao-gen entity would.
type studioRecord struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Name  string   `json:"name,omitempty" dgraph:"index=exact"`
}

func (s *studioRecord) RecordTypeName() string { return "studioRecord" }

type studioWrapper struct{ inner *studioRecord }

func (w *studioWrapper) Record() *studioRecord { return w.inner }

// TestClientUnwrapsWrapperThroughRealMutation exercises the real client path,
// not AsRecord in isolation: it inserts a wrapper and reads it back. If a
// mutation method stopped calling AsRecord, the wrapper (which has no usable
// dgraph fields of its own) would not persist Name and the inner UID would stay
// empty — so this test fails on that regression.
func TestClientUnwrapsWrapperThroughRealMutation(t *testing.T) {
	client, err := dg.NewClient("file://"+GetTempDir(t), dg.WithAutoSchema(true))
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	inner := &studioRecord{Name: "Acme"}
	wrapper := &studioWrapper{inner: inner}

	require.NoError(t, client.Insert(ctx, wrapper))
	require.NotEmpty(t, inner.UID,
		"Insert did not route the wrapper to its inner record")

	var got studioRecord
	require.NoError(t, client.Get(ctx, &got, inner.UID))
	require.Equal(t, "Acme", got.Name)
}

// TestClientUnwrapsWrapperSliceThroughRealMutation covers the batch path:
// Insert accepts "an object or slice of objects", so a []*wrapper must have
// each element unwrapped. Before AsRecord mapped over slices, dgman
// reflected over the wrappers and failed with "cannot set uid/", persisting
// nothing; now each inner record is inserted and receives a UID.
func TestClientUnwrapsWrapperSliceThroughRealMutation(t *testing.T) {
	client, err := dg.NewClient("file://"+GetTempDir(t), dg.WithAutoSchema(true))
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	acme := &studioRecord{Name: "Acme"}
	globex := &studioRecord{Name: "Globex"}
	batch := []*studioWrapper{{inner: acme}, {inner: globex}}

	require.NoError(t, client.Insert(ctx, batch))
	require.NotEmpty(t, acme.UID,
		"Insert did not route the first wrapper to its inner record")
	require.NotEmpty(t, globex.UID,
		"Insert did not route the second wrapper to its inner record")
	require.NotEqual(t, acme.UID, globex.UID,
		"batch elements should receive distinct UIDs")

	var gotAcme, gotGlobex studioRecord
	require.NoError(t, client.Get(ctx, &gotAcme, acme.UID))
	require.NoError(t, client.Get(ctx, &gotGlobex, globex.UID))
	require.Equal(t, "Acme", gotAcme.Name)
	require.Equal(t, "Globex", gotGlobex.Name)
}
