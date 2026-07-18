/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"errors"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/go-logr/stdr"
	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/require"

	dg "github.com/dgraph-io/dgdao"
)

// txnDoc is a node with a unique/upsert scalar, a plain scalar, and a self edge,
// exercising every TxnContext write and delete method.
type txnDoc struct {
	UID     string    `json:"uid,omitempty"`
	Name    string    `json:"name,omitempty" dgraph:"index=exact upsert unique"`
	Note    string    `json:"note,omitempty" dgraph:"index=term"`
	Related []*txnDoc `json:"related,omitempty"`
	DType   []string  `json:"dgraph.type,omitempty"`
}

// txnUser carries a validate:"required" scalar so validation can be exercised
// inside a transaction. Name is also the upsert key.
type txnUser struct {
	UID   string   `json:"uid,omitempty"`
	Name  string   `json:"name,omitempty" dgraph:"index=exact upsert unique" validate:"required"`
	Email string   `json:"email,omitempty" dgraph:"index=exact"`
	DType []string `json:"dgraph.type,omitempty"`
}

// txnURICases returns the file:// (embedded) and dgraph:// (remote) targets,
// mirroring the table used across the suite. The remote case is skipped unless
// DGDAO_TEST_ADDR is set, so the -short unit job runs the embedded case only.
func txnURICases(t *testing.T) []struct {
	name string
	uri  string
	skip bool
} {
	return []struct {
		name string
		uri  string
		skip bool
	}{
		{name: "FileURI", uri: "file://" + GetTempDir(t)},
		{
			name: "DgraphURI",
			uri:  "dgraph://" + os.Getenv("DGDAO_TEST_ADDR"),
			skip: os.Getenv("DGDAO_TEST_ADDR") == "",
		},
	}
}

// newValidatorClient builds an autoSchema client with a struct validator and a
// cleanup that drops data and resets the embedded engine, matching the pattern
// in CreateTestClient (which does not attach a validator).
func newValidatorClient(t *testing.T, uri string, v dg.StructValidator) (dg.Client, func()) {
	logger := stdr.NewWithOptions(log.New(os.Stdout, "", log.LstdFlags), stdr.Options{LogCaller: stdr.All}).WithName("dg")
	stdr.SetVerbosity(0)

	client, err := dg.NewClient(uri, dg.WithAutoSchema(true), dg.WithLogger(logger), dg.WithValidator(v))
	require.NoError(t, err)

	if strings.HasPrefix(uri, "dgraph://") {
		if err := client.DropAll(context.Background()); err != nil {
			t.Logf("Warning: failed to drop data at test start: %v", err)
		}
	}

	cleanup := func() {
		if err := client.DropAll(context.Background()); err != nil {
			t.Error(err)
		}
		client.Close()
		dg.Shutdown()
	}
	return client, cleanup
}

// Test case 1: validation fires inside the transaction. An Upsert of a struct
// missing a validate:"required" field returns an error naming the field, no UID
// is assigned, and committing lands nothing.
func TestTxnContext_ValidationFires(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := newValidatorClient(t, tc.uri, validator.New())
			defer cleanup()

			ctx := context.Background()

			// Establish schema and a baseline node so "landed nothing" is
			// verifiable as a stable count.
			valid := &txnUser{Name: "Valid User", Email: "valid@example.com"}
			require.NoError(t, client.Insert(ctx, valid))

			txn := client.NewTxnContext(ctx)
			defer txn.Discard()
			sc := client.InTxn(txn)

			invalid := &txnUser{Email: "no-name@example.com"} // Name is required.
			err := sc.Upsert(ctx, invalid)
			require.Error(t, err, "Upsert of an invalid struct should fail validation")
			require.Contains(t, err.Error(), "Name", "validation error should name the failed field")
			require.Empty(t, invalid.UID, "no UID should be assigned when validation blocks the write")

			// Commit was never a valid step for the failed write; committing now
			// must not resurrect it.
			require.NoError(t, txn.Commit())

			var users []txnUser
			require.NoError(t, client.Query(ctx, txnUser{}).Nodes(&users))
			require.Len(t, users, 1, "only the baseline node should exist; the invalid write must not land")
		})
	}
}

// Test case 2: several mutations commit atomically as one transaction, and an
// operation that errors before Commit lands nothing.
func TestTxnContext_MultiOpCommit(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			ctx := context.Background()

			// Set up via single-shot writes: a "main" node with a related edge,
			// plus a "victim" node to delete.
			main := &txnDoc{Name: "main", Note: "keep", Related: []*txnDoc{{Name: "friend"}}}
			require.NoError(t, client.Insert(ctx, main))
			require.NotEmpty(t, main.UID)
			require.NotEmpty(t, main.Related[0].UID, "related node should get a UID")

			victim := &txnDoc{Name: "victim", Note: "delete me"}
			require.NoError(t, client.Insert(ctx, victim))
			require.NotEmpty(t, victim.UID)

			// One transaction: drop main's edge, delete the victim node, add a
			// new node, then commit all three together.
			txn := client.NewTxnContext(ctx)
			defer txn.Discard()
			sc := client.InTxn(txn)
			require.NoError(t, txn.DeleteEdge(main.UID, "related"))
			require.NoError(t, txn.DeleteNode(victim.UID))
			upserted := &txnDoc{Name: "upserted", Note: "new"}
			require.NoError(t, sc.Upsert(ctx, upserted))
			require.NoError(t, txn.Commit())

			// main kept its scalars but lost the edge.
			var gotMain txnDoc
			require.NoError(t, client.Get(ctx, &gotMain, main.UID))
			require.Equal(t, "main", gotMain.Name)
			require.Empty(t, gotMain.Related, "the related edge should be gone")

			// victim is gone.
			var victims []txnDoc
			require.NoError(t, client.Query(ctx, txnDoc{}).Filter(`eq(name, "victim")`).Nodes(&victims))
			require.Empty(t, victims, "victim node should be deleted")

			// the upserted node landed.
			var ups []txnDoc
			require.NoError(t, client.Query(ctx, txnDoc{}).Filter(`eq(name, "upserted")`).Nodes(&ups))
			require.Len(t, ups, 1, "the upserted node should exist after commit")

			// Sibling: an operation that errors before Commit lands nothing.
			// Inserting a duplicate of the unique "name" predicate is rejected by
			// the unique check before any mutation is applied, so the store is
			// unchanged after Discard. This holds against both the embedded engine
			// and a real cluster. (The embedded engine commits each successful
			// mutation immediately — engine.go applies and commits per mutation —
			// so rollback of already-staged *valid* writes is only observable
			// against a real Dgraph cluster via the dgraph:// job.)
			var before []txnDoc
			require.NoError(t, client.Query(ctx, txnDoc{}).Nodes(&before))

			txn2 := client.NewTxnContext(ctx)
			sc2 := client.InTxn(txn2)
			require.Error(t, sc2.Insert(ctx, &txnDoc{Name: "upserted", Note: "dupe"}),
				"duplicate unique name is rejected before commit")
			txn2.Discard()

			var after []txnDoc
			require.NoError(t, client.Query(ctx, txnDoc{}).Nodes(&after))
			require.Len(t, after, len(before), "discarded transaction must not change the store")
		})
	}
}

// Test case 3: a duplicate insert of a unique predicate inside the transaction
// returns a typed *UniqueError, not a raw Dgraph error string.
func TestTxnContext_UniqueErrorTranslation(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			ctx := context.Background()

			first := &txnDoc{Name: "dup", Note: "first"}
			require.NoError(t, client.Insert(ctx, first))

			txn := client.NewTxnContext(ctx)
			defer txn.Discard()
			sc := client.InTxn(txn)

			dupe := &txnDoc{Name: "dup", Note: "second"}
			err := sc.Insert(ctx, dupe)
			require.Error(t, err, "a duplicate unique value should fail")

			var uniqueErr *dg.UniqueError
			require.True(t, errors.As(err, &uniqueErr), "error should be a typed *UniqueError, got %v", err)
			require.Equal(t, "name", uniqueErr.Field, "the violated predicate should be reported")
		})
	}
}

// Test case 4: DeletePredicate clears a scalar predicate's value inside the
// transaction; after commit has(predicate) is false and the value is gone while
// the node itself survives.
func TestTxnContext_DeletePredicate(t *testing.T) {
	for _, tc := range txnURICases(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			ctx := context.Background()

			doc := &txnDoc{Name: "with-note", Note: "delete this note"}
			require.NoError(t, client.Insert(ctx, doc))
			require.NotEmpty(t, doc.UID)

			txn := client.NewTxnContext(ctx)
			defer txn.Discard()
			require.NoError(t, txn.DeletePredicate(doc.UID, "note"))
			require.NoError(t, txn.Commit())

			// The node survives; only the note value is gone.
			var got txnDoc
			require.NoError(t, client.Get(ctx, &got, doc.UID))
			require.Equal(t, "with-note", got.Name, "node should still exist")
			require.Empty(t, got.Note, "note value should be gone")

			// has(note) is false: no node carries the predicate anymore.
			raw, err := client.QueryRaw(ctx, `{ q(func: has(note)) { uid } }`, nil)
			require.NoError(t, err)
			require.NotContains(t, string(raw), doc.UID, "no node should have the note predicate")
		})
	}
}
