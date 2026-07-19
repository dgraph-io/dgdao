/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/dgdao/typed"
)

// The typed InTxn tests live in the root package rather than in package typed
// because CI runs the unit suite as `go test -short -race -v .` against the root
// package only, with a dgraph:// matrix (DGDAO_TEST_ADDR) that activates the
// remote cases. Placing them here runs the typed txn-scoped client — including
// the WhereEdge-in-txn read-set guarantee — under both backends in CI.

// typedItem is a simple typed-client subject with an upsert key.
type typedItem struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Name  string   `json:"name,omitempty" dgraph:"index=exact upsert unique"`
	Qty   int      `json:"qty,omitempty" dgraph:"index=int"`
}

// edgeOwner/edgePet exercise the typed WhereEdge pre-pass across a transaction:
// owner has an outbound "pets" edge, and pet.Name is indexed so an edge filter
// eq(name, ...) resolves. They mirror the owner/pet pair in the typed package's
// own WhereEdge tests.
type edgeOwner struct {
	UID   string     `json:"uid,omitempty"`
	DType []string   `json:"dgraph.type,omitempty"`
	Name  string     `json:"name,omitempty" dgraph:"index=exact"`
	Pets  []*edgePet `json:"pets,omitempty"`
}

type edgePet struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Name  string   `json:"name,omitempty" dgraph:"index=exact"`
}

// TestInTxn_Typed_WritesStageAndCommit stages typed writes through the
// txn-scoped typed client, commits, and verifies each landed via a fresh typed
// client. It proves typed Add/Upsert route through the transaction.
func TestInTxn_Typed_WritesStageAndCommit(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()
			ctx := context.Background()

			items := typed.NewClient[typedItem](client)
			// Establish schema (Txn does not run autoSchema).
			require.NoError(t, items.Insert(ctx, &typedItem{Name: "schema-seed", Qty: 0}))

			txn := client.NewTxn(ctx)
			defer txn.Discard()
			scoped := items.InTxn(txn)

			added := &typedItem{Name: "added", Qty: 1}
			require.NoError(t, scoped.Insert(ctx, added))
			require.NotEmpty(t, added.UID, "typed Add in-txn should populate the UID")

			require.NoError(t, scoped.Upsert(ctx, &typedItem{Name: "upserted", Qty: 7}, "name"))

			require.NoError(t, txn.Commit())

			// A fresh typed client sees both committed writes.
			got, err := items.Get(ctx, added.UID)
			require.NoError(t, err)
			require.Equal(t, 1, got.Qty)

			ups, err := items.Query(ctx).Filter(`eq(name, "upserted")`).Nodes()
			require.NoError(t, err)
			require.Len(t, ups, 1)
			require.Equal(t, 7, ups[0].Qty)
		})
	}
}

// TestInTxn_Typed_QueryReadsThroughTxn drives a typed query through the
// txn-scoped client against committed data, proving the typed read path runs on
// the transaction. Reads only, so it holds on both backends.
func TestInTxn_Typed_QueryReadsThroughTxn(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()
			ctx := context.Background()

			items := typed.NewClient[typedItem](client)
			require.NoError(t, items.Insert(ctx, &typedItem{Name: "findme", Qty: 42}))

			txn := client.NewTxn(ctx)
			defer txn.Discard()
			scoped := items.InTxn(txn)

			got, err := scoped.Query(ctx).Filter(`eq(name, "findme")`).Nodes()
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Equal(t, 42, got[0].Qty)
		})
	}
}

// TestInTxn_Typed_WhereEdgeRunsInTxn is the correctness crux: a typed WhereEdge
// query built from a txn-scoped client runs its edge-match pre-pass and its data
// block through the SAME transaction. Against committed data this holds on both
// backends (the whole WhereEdge request is one QueryRaw on the txn). The
// discriminating assertion — that a write staged inside the txn is visible to
// the WhereEdge pre-pass — is observable only on a real cluster (read-your-writes
// across an interactive txn); a pre-pass that split to a fresh read-only
// connection would not see the uncommitted owner and the query would return
// nothing.
func TestInTxn_Typed_WhereEdgeRunsInTxn(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()
			ctx := context.Background()

			pets := typed.NewClient[edgePet](client)
			owners := typed.NewClient[edgeOwner](client)

			// Seed two owner/pet pairs; only Alice owns "Fido".
			fido := &edgePet{Name: "Fido"}
			require.NoError(t, pets.Insert(ctx, fido))
			require.NoError(t, owners.Insert(ctx, &edgeOwner{Name: "Alice", Pets: []*edgePet{fido}}))
			rex := &edgePet{Name: "Rex"}
			require.NoError(t, pets.Insert(ctx, rex))
			require.NoError(t, owners.Insert(ctx, &edgeOwner{Name: "Bob", Pets: []*edgePet{rex}}))

			txn := client.NewTxn(ctx)
			defer txn.Discard()
			scopedOwners := owners.InTxn(txn)

			// The WhereEdge pre-pass + data block execute as one request on the txn.
			got, err := scopedOwners.Query(ctx).
				WhereEdge("pets", `eq(name, $1)`, "Fido").
				Nodes()
			require.NoError(t, err)
			require.Len(t, got, 1, "WhereEdge in-txn should match exactly Alice")
			require.Equal(t, "Alice", got[0].Name)

			if !isRemote(tc.uri) {
				return
			}

			// Real cluster: a node staged inside the txn is visible to the WhereEdge
			// pre-pass, proving the pre-pass shares the txn's read-set. If it read on
			// a fresh connection, the uncommitted Zoe/Ghost pair would be invisible
			// and this query would return nothing.
			scopedPets := pets.InTxn(txn)
			ghost := &edgePet{Name: "Ghost"}
			require.NoError(t, scopedPets.Insert(ctx, ghost))
			require.NoError(t, scopedOwners.Insert(ctx, &edgeOwner{Name: "Zoe", Pets: []*edgePet{ghost}}))

			staged, err := scopedOwners.Query(ctx).
				WhereEdge("pets", `eq(name, $1)`, "Ghost").
				Nodes()
			require.NoError(t, err)
			require.Len(t, staged, 1, "WhereEdge pre-pass must see the node staged in the same txn")
			require.Equal(t, "Zoe", staged[0].Name)
		})
	}
}

// TestInTxn_Typed_GuardedReadDeleteCommit exercises the guarded read-then-delete
// the WhereEdge-in-txn guarantee protects: within one transaction, find owners
// by an edge constraint, delete a matched owner, and commit atomically.
func TestInTxn_Typed_GuardedReadDeleteCommit(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()
			ctx := context.Background()

			pets := typed.NewClient[edgePet](client)
			owners := typed.NewClient[edgeOwner](client)

			fido := &edgePet{Name: "Fido"}
			require.NoError(t, pets.Insert(ctx, fido))
			require.NoError(t, owners.Insert(ctx, &edgeOwner{Name: "Alice", Pets: []*edgePet{fido}}))

			txn := client.NewTxn(ctx)
			defer txn.Discard()
			scopedOwners := owners.InTxn(txn)

			// Guarded read: find Fido's owners via the WhereEdge pre-pass (in-txn).
			matched, err := scopedOwners.Query(ctx).
				WhereEdge("pets", `eq(name, $1)`, "Fido").
				Nodes()
			require.NoError(t, err)
			require.Len(t, matched, 1)

			// Delete the matched owner in the same txn and commit atomically.
			require.NoError(t, scopedOwners.Delete(ctx, matched[0].UID))
			require.NoError(t, txn.Commit())

			remaining, err := owners.Query(ctx).Filter(`eq(name, "Alice")`).Nodes()
			require.NoError(t, err)
			require.Empty(t, remaining, "the guarded owner must be deleted after commit")
		})
	}
}
