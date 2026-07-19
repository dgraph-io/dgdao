/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	dg "github.com/dgraph-io/dgdao"
)

// isRemote reports whether a URI case targets a real Dgraph cluster. A handful
// of guarantees — read-your-writes across an interactive transaction and
// abort-on-conflict — are observable only against a real cluster: the embedded
// engine applies and commits each mutation immediately at its own timestamp, so
// a read issued after a write on the same interactive transaction cannot share
// that transaction's snapshot. Those assertions are gated behind isRemote and
// run in the dgraph:// CI job, exactly as v0.7.0's rollback coverage is.
func isRemote(uri string) bool { return strings.HasPrefix(uri, "dgraph://") }

// TestInTxn_WritesStageAndCommit stages every untyped write through the
// txn-scoped client, commits once, and confirms each landed via a fresh
// (non-txn) client read. It proves the scoped writes route through the caller's
// transaction and commit together, and that validation runs on the staged path.
func TestInTxn_WritesStageAndCommit(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()
			ctx := context.Background()

			// Txn does not run autoSchema; establish the txnDoc schema and a
			// deletion target with single-shot writes first.
			require.NoError(t, client.Insert(ctx, &txnDoc{Name: "seed", Note: "seed"}))
			victim := &txnDoc{Name: "victim", Note: "die"}
			require.NoError(t, client.Insert(ctx, victim))

			txn := client.NewTxn(ctx)
			defer txn.Discard()
			sc := client.InTxn(txn)

			// A chain of writes with no interleaved read, staged on the transaction.
			ins := &txnDoc{Name: "ins", Note: "note-1"}
			require.NoError(t, sc.Insert(ctx, ins))
			require.NotEmpty(t, ins.UID, "Insert should populate the UID")

			ins.Note = "note-2"
			require.NoError(t, sc.Update(ctx, ins))

			up := &txnDoc{Name: "ups", Note: "up-1"}
			require.NoError(t, sc.Upsert(ctx, up))
			require.NotEmpty(t, up.UID, "Upsert should populate the UID")

			require.NoError(t, sc.Delete(ctx, []string{victim.UID}))

			require.NoError(t, txn.Commit())

			// A fresh client read reflects every staged op after commit.
			var gotIns txnDoc
			require.NoError(t, client.Get(ctx, &gotIns, ins.UID))
			require.Equal(t, "note-2", gotIns.Note, "committed Insert+Update should persist")

			var ups []txnDoc
			require.NoError(t, client.Query(ctx, txnDoc{}).Filter(`eq(name, "ups")`).Nodes(&ups))
			require.Len(t, ups, 1, "committed Upsert should persist")

			var victims []txnDoc
			require.NoError(t, client.Query(ctx, txnDoc{}).Filter(`eq(name, "victim")`).Nodes(&victims))
			require.Empty(t, victims, "committed Delete should remove the node")
		})
	}
}

// TestInTxn_ReadsRouteThroughTxn drives each untyped read — Get, Query,
// QueryRaw — through the scoped client against committed data, proving the read
// surface executes on the transaction. Reads only, so no mutation advances the
// read timestamp mid-transaction; this holds on the embedded engine and a real
// cluster alike.
func TestInTxn_ReadsRouteThroughTxn(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()
			ctx := context.Background()

			seed := &txnDoc{Name: "reader", Note: "payload"}
			require.NoError(t, client.Insert(ctx, seed))

			txn := client.NewTxn(ctx)
			defer txn.Discard()
			sc := client.InTxn(txn)

			var got txnDoc
			require.NoError(t, sc.Get(ctx, &got, seed.UID))
			require.Equal(t, "payload", got.Note, "Get in-txn must read committed data")

			var q []txnDoc
			require.NoError(t, sc.Query(ctx, txnDoc{}).Filter(`eq(name, "reader")`).Nodes(&q))
			require.Len(t, q, 1, "Query in-txn must read committed data")

			raw, err := sc.QueryRaw(ctx, `{ q(func: eq(name, "reader")) { uid name } }`, nil)
			require.NoError(t, err)
			require.Contains(t, string(raw), "reader", "QueryRaw in-txn must read committed data")
		})
	}
}

// TestInTxn_ReadYourWrites proves the scoped client's reads join the
// transaction's read-set: a write staged earlier in the transaction is visible
// to a later read on the same scoped client, before Commit. A read that split to
// a fresh read-only connection would not see the uncommitted write. This is
// observable only against a real cluster (see isRemote).
func TestInTxn_ReadYourWrites(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}
			if !isRemote(tc.uri) {
				t.Skip("read-your-writes across an interactive txn is real-Dgraph-only")
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()
			ctx := context.Background()

			require.NoError(t, client.Insert(ctx, &txnDoc{Name: "schema-seed"}))

			txn := client.NewTxn(ctx)
			defer txn.Discard()
			sc := client.InTxn(txn)

			staged := &txnDoc{Name: "staged", Note: "uncommitted"}
			require.NoError(t, sc.Insert(ctx, staged))
			require.NotEmpty(t, staged.UID)

			var got txnDoc
			require.NoError(t, sc.Get(ctx, &got, staged.UID))
			require.Equal(t, "uncommitted", got.Note, "Get must see the write staged in the same txn")

			var q []txnDoc
			require.NoError(t, sc.Query(ctx, txnDoc{}).Filter(`eq(name, "staged")`).Nodes(&q))
			require.Len(t, q, 1, "Query must see the write staged in the same txn")
		})
	}
}

// TestInTxn_ReadDeleteCommitAtomic is the guarded read+delete path: within one
// transaction the scoped client reads a node, deletes it, stages an unrelated
// insert, and commits all three atomically. The read precedes the writes, so no
// mid-transaction read follows a mutation — the path holds on both backends.
func TestInTxn_ReadDeleteCommitAtomic(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()
			ctx := context.Background()

			require.NoError(t, client.Insert(ctx, &txnDoc{Name: "target", Note: "guarded"}))

			txn := client.NewTxn(ctx)
			defer txn.Discard()
			sc := client.InTxn(txn)

			// Read the target in-txn (this read joins the txn's read-set).
			var target []txnDoc
			require.NoError(t, sc.Query(ctx, txnDoc{}).Filter(`eq(name, "target")`).Nodes(&target))
			require.Len(t, target, 1)

			// Delete it and stage an unrelated insert in the same txn.
			require.NoError(t, sc.Delete(ctx, []string{target[0].UID}))
			require.NoError(t, sc.Insert(ctx, &txnDoc{Name: "replacement", Note: "new"}))

			require.NoError(t, txn.Commit())

			var targets []txnDoc
			require.NoError(t, client.Query(ctx, txnDoc{}).Filter(`eq(name, "target")`).Nodes(&targets))
			require.Empty(t, targets, "target must be deleted after commit")

			var reps []txnDoc
			require.NoError(t, client.Query(ctx, txnDoc{}).Filter(`eq(name, "replacement")`).Nodes(&reps))
			require.Len(t, reps, 1, "replacement must be inserted after commit")
		})
	}
}

// TestInTxn_GetOrInsert covers both GetOrInsert outcomes inside a txn: the
// create path (no match, loaded=false) and the load path (existing match,
// loaded=true, obj hydrated from the stored node). MutateOrGet echoes the
// transaction's timestamp rather than advancing it, so both calls succeed on
// either backend.
func TestInTxn_GetOrInsert(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()
			ctx := context.Background()

			require.NoError(t, client.Insert(ctx, &txnDoc{Name: "existing", Note: "original"}))

			txn := client.NewTxn(ctx)
			defer txn.Discard()
			sc := client.InTxn(txn)

			// Create path: no node named "fresh" exists yet.
			fresh := &txnDoc{Name: "fresh", Note: "brand-new"}
			loaded, err := sc.GetOrInsert(ctx, fresh)
			require.NoError(t, err)
			require.False(t, loaded, "GetOrInsert of an absent key must store, not load")
			require.NotEmpty(t, fresh.UID)

			// Load path: a node named "existing" already exists.
			dup := &txnDoc{Name: "existing", Note: "ignored"}
			loaded, err = sc.GetOrInsert(ctx, dup)
			require.NoError(t, err)
			require.True(t, loaded, "GetOrInsert of a present key must load, not store")
			require.Equal(t, "original", dup.Note, "loaded obj must be hydrated from the stored node")

			require.NoError(t, txn.Commit())

			var all []txnDoc
			require.NoError(t, client.Query(ctx, txnDoc{}).Nodes(&all))
			require.Len(t, all, 2, "exactly the seed and the freshly-stored node should exist")
		})
	}
}

// TestInTxn_GetAndDelete reads-and-consumes a node inside a txn: the matched
// node is hydrated into obj and its deletion staged, landing on Commit. A
// separate transaction confirms a miss returns loaded=false and leaves obj zero.
func TestInTxn_GetAndDelete(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()
			ctx := context.Background()

			require.NoError(t, client.Insert(ctx, &txnDoc{Name: "consume-me", Note: "payload"}))

			txn := client.NewTxn(ctx)
			defer txn.Discard()
			sc := client.InTxn(txn)

			var got txnDoc
			loaded, err := sc.GetAndDelete(ctx, &got, "consume-me")
			require.NoError(t, err)
			require.True(t, loaded, "the seeded node should be consumed")
			require.Equal(t, "payload", got.Note, "obj must be hydrated with the consumed node")

			require.NoError(t, txn.Commit())

			var remaining []txnDoc
			require.NoError(t, client.Query(ctx, txnDoc{}).Filter(`eq(name, "consume-me")`).Nodes(&remaining))
			require.Empty(t, remaining, "the consumed node must be gone after commit")

			// A miss in a fresh transaction returns loaded=false and zeroes obj.
			txn2 := client.NewTxn(ctx)
			defer txn2.Discard()
			sc2 := client.InTxn(txn2)

			miss := txnDoc{Name: "pre-populated"}
			loaded, err = sc2.GetAndDelete(ctx, &miss, "does-not-exist")
			require.NoError(t, err)
			require.False(t, loaded, "a miss must report loaded=false")
			require.Empty(t, miss.Name, "obj must be zeroed on a miss")
		})
	}
}

// TestInTxn_ValidationAndUniqueError proves the scoped client's writes run the
// same pre-mutation pipeline as the single-shot methods: struct validation fires
// before staging, and a duplicate unique value returns a typed *UniqueError
// rather than a raw Dgraph error string.
func TestInTxn_ValidationAndUniqueError(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := newValidatorClient(t, tc.uri, dg.NewValidator())
			defer cleanup()
			ctx := context.Background()

			require.NoError(t, client.Insert(ctx, &txnUser{Name: "taken", Email: "a@example.com"}))

			txn := client.NewTxn(ctx)
			defer txn.Discard()
			sc := client.InTxn(txn)

			// Validation fires: Name is validate:"required".
			err := sc.Upsert(ctx, &txnUser{Email: "no-name@example.com"})
			require.Error(t, err, "missing required field must fail validation")
			require.Contains(t, err.Error(), "Name")

			// Unique violation returns a typed *UniqueError.
			err = sc.Insert(ctx, &txnUser{Name: "taken", Email: "b@example.com"})
			require.Error(t, err)
			var uniqueErr *dg.UniqueError
			require.True(t, errors.As(err, &uniqueErr), "expected a typed *UniqueError, got %v", err)
			require.Equal(t, "name", uniqueErr.Field)
		})
	}
}

// TestInTxn_PackageLevelConstructor proves the package-level dgdao.InTxn — the
// constructor the typed layer uses — produces a ClientTxn bound to the same
// transaction as Client.InTxn: a write staged through it lands only on Commit,
// with defaults and validation applied by the originating client.
func TestInTxn_PackageLevelConstructor(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()
			ctx := context.Background()

			// Txn does not run autoSchema; establish the txnDoc schema first.
			require.NoError(t, client.Insert(ctx, &txnDoc{Name: "seed", Note: "seed"}))

			txn := client.NewTxn(ctx)
			defer txn.Discard()
			sc := dg.InTxn(txn)

			doc := &txnDoc{Name: "pkg-ctor", Note: "n"}
			require.NoError(t, sc.Insert(ctx, doc))
			require.NotEmpty(t, doc.UID, "Insert should populate the UID")
			require.NoError(t, txn.Commit())

			var got txnDoc
			require.NoError(t, client.Get(ctx, &got, doc.UID))
			require.Equal(t, "pkg-ctor", got.Name, "write staged via dgdao.InTxn should land on Commit")
		})
	}
}
