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

// lockLikeNode mirrors dgdao-migrate's migrationLock: its Dgraph type
// name (declared via the DType tag, "DTypeMismatchType") differs from its Go
// struct name ("lockLikeNode").
type lockLikeNode struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty" dgraph:"DTypeMismatchType"`
	Name  string   `json:"dtypeMismatchName,omitempty" dgraph:"predicate=dtype_mismatch_name index=exact"`
}

// TestMutateValidation_ResolvesDTypeName guards the AutoSchema-disabled mutation
// path: it must resolve the type name from the DType tag (the same way schema
// generation and getNodeType do), not from the raw Go struct name. Before the
// fix, Insert rejected the row with "schema validation failed: database schema
// does not contain type lockLikeNode" even though the schema held the (correctly
// named) "type DTypeMismatchType".
func TestMutateValidation_ResolvesDTypeName(t *testing.T) {
	client, err := dg.NewClient("file://" + GetTempDir(t)) // autoSchema disabled by default
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	// Register the type under its Dgraph name (derived from the DType tag).
	require.NoError(t, client.UpdateSchema(ctx, &lockLikeNode{}))

	// Insert must pass validation now that the type name resolves via DType.
	err = client.Insert(ctx, &lockLikeNode{DType: []string{"DTypeMismatchType"}, Name: "x"})
	require.NoError(t, err)
}
