/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"os"
	"testing"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/stretchr/testify/require"
)

// TestDropPredicateEmbedded exercises the schema-DDL surface end-to-end:
// Client.AlterSchema declares a raw predicate, and a gRPC Alter with DropAttr
// routes through embedded_client.go's DropAttr arm into engine.dropPredicate.
func TestDropPredicateEmbedded(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "DropPredicateWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "DropPredicateWithDgraphURI",
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

			// Declare an indexed string predicate and insert a node carrying it.
			err := client.AlterSchema(ctx, "dropme: string @index(exact) .")
			require.NoError(t, err, "AlterSchema should succeed")

			dg, dgCleanup, err := client.DgraphClient()
			require.NoError(t, err, "DgraphClient should succeed")
			defer dgCleanup()

			_, err = dg.NewTxn().Mutate(ctx, &api.Mutation{
				SetJson:   []byte(`[{"dropme":"hello"}]`),
				CommitNow: true,
			})
			require.NoError(t, err, "mutate should succeed")

			// Confirm the predicate is present before the drop.
			raw, err := client.QueryRaw(ctx, `{ q(func: has(dropme)) { c: count(uid) } }`, nil)
			require.NoError(t, err, "count query should succeed")
			require.Contains(t, string(raw), `"c":1`, "predicate present before drop")

			// Drop the predicate via the public path; this exercises
			// embedded_client.go's DropAttr arm + engine.dropPredicate.
			err = dg.Alter(ctx, &api.Operation{DropAttr: "dropme"})
			require.NoError(t, err, "DropAttr should succeed")

			// Confirm the data is gone (no nodes have the predicate).
			raw, err = client.QueryRaw(ctx, `{ q(func: has(dropme)) { c: count(uid) } }`, nil)
			require.NoError(t, err, "count query should succeed after drop")
			require.Contains(t, string(raw), `"c":0`, "predicate values gone after drop")

			// Confirm the schema entry is gone.
			raw, err = client.QueryRaw(ctx, `schema(pred: [dropme]) { type }`, nil)
			require.NoError(t, err, "schema query should succeed after drop")
			require.NotContains(t, string(raw), "dropme", "predicate schema entry gone after drop")
		})
	}
}
