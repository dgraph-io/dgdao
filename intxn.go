/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"context"
	"errors"
	"fmt"

	"github.com/dolan-in/dgman/v2"
)

// txnClient is the Client returned by Client.InTxn: a client whose reads and
// writes route through a caller-supplied TxnContext instead of a fresh
// read-only or commit-now transaction. It embeds the originating client, so the
// non-transactional surface — schema management (UpdateSchema, AlterSchema,
// GetSchema), DropAll/DropData, Close, DgraphClient, WithRetry — is inherited
// unchanged; only the operations that must join the transaction (Query,
// QueryRaw, Get, Insert, InsertRaw, Upsert, Update, Delete, LoadOrStore,
// LoadAndDelete) are overridden to run on tx. Schema and drop operations are
// deliberately left non-transactional because Dgraph Alter is not part of a
// transaction.
//
// A txnClient is a value type sharing one *TxnContext pointer across copies, so
// passing it by value is safe: every copy stages onto and reads from the same
// transaction.
type txnClient struct {
	client
	tx *TxnContext
}

// InTxn returns a txn-scoped Client whose entire surface routes through tx. See
// the Client.InTxn interface documentation for the full contract. The scoped
// client is bound to the client that created tx, so its options (validator,
// max edge traversal, embedding provider) match that client regardless of which
// client value InTxn is called on.
func (c client) InTxn(tx *TxnContext) Client {
	return txnClient{client: tx.c, tx: tx}
}

// Query reads within the transaction (read-your-writes over staged mutations),
// mirroring Client.Query. The transaction governs the read context; the ctx
// argument is accepted for interface parity. Returns nil if the transaction
// failed to acquire a connection.
func (tc txnClient) Query(_ context.Context, model any) *dgman.Query {
	return tc.tx.query(model)
}

// QueryRaw runs a raw DQL query within the transaction, mirroring
// Client.QueryRaw. The query observes writes already staged on the same txn.
func (tc txnClient) QueryRaw(ctx context.Context, q string, vars map[string]string) ([]byte, error) {
	return tc.tx.queryRaw(ctx, q, vars)
}

// Get reads a single object by UID within the transaction, mirroring Client.Get.
func (tc txnClient) Get(ctx context.Context, obj any, uid string) error {
	return tc.tx.get(ctx, obj, uid)
}

// Insert stages an insert of obj on the transaction after applying defaults and
// validating, mirroring Client.Insert minus the immediate commit.
func (tc txnClient) Insert(ctx context.Context, obj any) error {
	return tc.stage(ctx, obj, true, func(o any) ([]string, error) {
		return tc.tx.txn.MutateBasic(o)
	})
}

// InsertRaw stages an insert of obj on the transaction after validating (no
// default population), mirroring Client.InsertRaw.
//
// Deprecated: InsertRaw is identical to Insert apart from skipping default
// population. Use Insert instead.
func (tc txnClient) InsertRaw(ctx context.Context, obj any) error {
	return tc.stage(ctx, obj, false, func(o any) ([]string, error) {
		return tc.tx.txn.MutateBasic(o)
	})
}

// Upsert stages an insert-or-update of obj on the transaction after applying
// defaults and validating, mirroring Client.Upsert. With no predicates, the
// first field tagged dgraph:"upsert" is used as the match key.
func (tc txnClient) Upsert(ctx context.Context, obj any, predicates ...string) error {
	return tc.stage(ctx, obj, true, func(o any) ([]string, error) {
		return tc.tx.txn.Upsert(o, predicates...)
	})
}

// Update stages an update of obj on the transaction after applying defaults and
// validating, mirroring Client.Update. obj must carry a UID.
func (tc txnClient) Update(ctx context.Context, obj any) error {
	return tc.stage(ctx, obj, true, func(o any) ([]string, error) {
		return tc.tx.txn.MutateBasic(o)
	})
}

// Delete stages deletion of the given UIDs on the transaction, mirroring
// Client.Delete minus the immediate commit.
func (tc txnClient) Delete(_ context.Context, uids []string) error {
	if tc.tx.initErr != nil {
		return tc.tx.initErr
	}
	return tc.tx.txn.DeleteNode(uids...)
}

// LoadOrStore stages obj only if no node matches the upsert predicates,
// reporting loaded=true when an existing node already matched (obj is then
// hydrated from it). It mirrors Client.LoadOrStore but stages on the
// transaction: the store lands on Commit rather than immediately.
func (tc txnClient) LoadOrStore(ctx context.Context, obj any, predicates ...string) (loaded bool, err error) {
	if tc.tx.initErr != nil {
		return false, tc.tx.initErr
	}
	obj = UnwrapSchema(obj)
	if err := checkPointer(obj); err != nil {
		return false, err
	}
	// Apply defaults before validation, so a defaulted field can satisfy a
	// validate:"required" rule — matching the single-shot ordering.
	if err := tc.applyDefaults(ctx, obj); err != nil {
		return false, err
	}
	if err := tc.validateStruct(ctx, obj); err != nil {
		return false, err
	}
	uids, err := tc.tx.txn.MutateOrGet(obj, predicates...)
	if err != nil {
		if uniqueErr := parseUniqueError(err); uniqueErr != nil {
			return false, uniqueErr
		}
		return false, err
	}
	// MutateOrGet returns created UIDs only; empty => an existing node matched.
	return len(uids) == 0, nil
}

// LoadAndDelete reads the node whose key predicate equals key into obj and
// stages its deletion, returning loaded=false (and leaving obj zero) when none
// matched. It mirrors Client.LoadAndDelete, but because the read and delete
// already run inside the caller's transaction, it neither commits nor retries:
// the enclosing transaction owns commit, and conflict resolution against a
// concurrent consumer happens when that transaction commits (a real Dgraph
// cluster aborts the loser). With no predicates, the first dgraph:"upsert" field
// is used.
func (tc txnClient) LoadAndDelete(_ context.Context, obj any, key any, predicates ...string) (loaded bool, err error) {
	if tc.tx.initErr != nil {
		return false, tc.tx.initErr
	}
	obj = UnwrapSchema(obj)
	if err := checkPointer(obj); err != nil {
		return false, err
	}

	pred := ""
	if len(predicates) > 0 {
		pred = predicates[0]
	} else {
		pred = firstUpsertPredicate(obj)
	}
	if pred == "" {
		return false, fmt.Errorf("LoadAndDelete: no key predicate (pass one or tag a field dgraph:\"upsert\")")
	}
	// The key value is parameterized ($1), but the predicate name is
	// concatenated straight into the DQL filter; reject anything that is not a
	// plain Dgraph predicate identifier before it reaches the filter.
	if !isValidPredicateName(pred) {
		return false, fmt.Errorf("LoadAndDelete: invalid key predicate %q (allowed: letters, digits, '_', '.', '-')", pred)
	}

	getErr := tc.tx.txn.Get(obj).
		Filter("eq("+pred+", $1)", key).
		All(tc.options.maxEdgeTraversal).
		Node()
	if getErr != nil {
		if errors.Is(getErr, dgman.ErrNodeNotFound) {
			// Honor the contract: obj is zero when loaded=false.
			zeroValue(obj)
			return false, nil
		}
		return false, getErr
	}

	uid := uidOf(obj)
	if uid == "" {
		return false, fmt.Errorf("LoadAndDelete: matched a node but read no UID; the model needs a string UID field")
	}
	if delErr := tc.tx.txn.DeleteNode(uid); delErr != nil {
		return false, delErr
	}
	return true, nil
}

// stage runs the shared pre-mutation pipeline — schema-wrapper unwrap, optional
// default population, struct validation — then the staged dgman write, and
// translates a Dgraph unique-constraint violation into a typed *UniqueError. It
// mirrors the pre-mutation steps of the single-shot Insert/Upsert/Update
// methods, so a staged write behaves exactly like a single-shot write minus the
// immediate commit.
func (tc txnClient) stage(ctx context.Context, obj any, applyDefaults bool, write func(any) ([]string, error)) error {
	if tc.tx.initErr != nil {
		return tc.tx.initErr
	}
	obj = UnwrapSchema(obj)
	if applyDefaults {
		if err := tc.applyDefaults(ctx, obj); err != nil {
			return err
		}
	}
	if err := tc.validateStruct(ctx, obj); err != nil {
		return err
	}
	if _, err := write(obj); err != nil {
		if uniqueErr := parseUniqueError(err); uniqueErr != nil {
			return uniqueErr
		}
		return err
	}
	return nil
}
