/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"context"

	"github.com/dgraph-io/dgo/v250"
	"github.com/dolan-in/dgman/v2"
)

// TxnContext is a validated, deferred-commit read-write transaction.
//
// The single-shot Client write methods (Insert, Upsert, Update) each open a
// dgman transaction with SetCommitNow, committing per call, so a caller cannot
// group several mutations into one atomic commit through the typed API. A
// TxnContext removes that limit: it opens one dgman transaction WITHOUT
// SetCommitNow, stages any number of mutations and deletes, and commits them
// together only when Commit is called.
//
// Every staged write runs the SAME pre-mutation pipeline the single-shot methods
// apply — default population (Defaulter) followed by struct validation
// (validate: tags and SelfValidator) — and the same unique-constraint error
// translation to a typed *UniqueError. Grouping mutations therefore costs no
// validation, which is the whole point: callers that need multi-mutation
// atomicity no longer have to drop to raw dgman and lose validation.
//
// A TxnContext holds a Dgraph connection from the client pool for its lifetime.
// The caller must release it by calling Commit or Discard exactly once; the
// idiomatic pattern is to defer Discard immediately after NewTxnContext, since
// Discard is a no-op after a successful Commit but still guarantees the
// connection is returned to the pool on error and panic paths.
type TxnContext struct {
	c   client
	ctx context.Context
	// txn is the underlying deferred-commit dgman transaction. It is nil only
	// when the pool failed to hand out a connection; initErr records that error.
	txn     *dgman.TxnContext
	conn    *dgo.Dgraph
	initErr error
	closed  bool
}

// NewTxnContext starts a validated, deferred-commit read-write transaction.
//
// It checks out a connection from the client pool and opens a dgman transaction
// without SetCommitNow, so staged mutations do not commit until Commit is
// called. The returned TxnContext is never nil: if the pool cannot supply a
// connection, the error is deferred and surfaces from the first write method or
// from Commit, keeping the constructor's signature free of an error return.
func (c client) NewTxnContext(ctx context.Context) *TxnContext {
	t := &TxnContext{c: c, ctx: ctx}
	conn, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		t.initErr = err
		return t
	}
	t.conn = conn
	// No SetCommitNow: mutations stage on the transaction until Commit.
	t.txn = dgman.NewTxnContext(ctx, conn)
	return t
}

// Insert stages an insert of obj (a pointer to a struct, or a slice of such
// pointers) on the transaction, after applying defaults and validating it.
func (t *TxnContext) Insert(obj any) error {
	return t.stageMutation(obj, func(obj any) ([]string, error) {
		return t.txn.MutateBasic(obj)
	})
}

// Upsert stages an insert-or-update of obj on the transaction, after applying
// defaults and validating it. With no predicates, the first field tagged
// dgraph:"upsert" is used as the match key.
func (t *TxnContext) Upsert(obj any, predicates ...string) error {
	return t.stageMutation(obj, func(obj any) ([]string, error) {
		return t.txn.Upsert(obj, predicates...)
	})
}

// Update stages an update of obj on the transaction, after applying defaults and
// validating it. As with the single-shot Client.Update, obj must carry a UID.
func (t *TxnContext) Update(obj any) error {
	return t.stageMutation(obj, func(obj any) ([]string, error) {
		return t.txn.MutateBasic(obj)
	})
}

// stageMutation runs the shared pre-mutation pipeline — schema-wrapper unwrap,
// default population, struct validation — then the staged dgman write, and
// translates a Dgraph unique-constraint violation into a typed *UniqueError.
// It mirrors the no-CommitNow path of client.process and the pre-mutation steps
// of the single-shot Insert/Upsert/Update methods, so a staged write behaves
// exactly like a single-shot write minus the immediate commit.
func (t *TxnContext) stageMutation(obj any, write func(any) ([]string, error)) error {
	if t.initErr != nil {
		return t.initErr
	}
	obj = UnwrapSchema(obj)
	// Apply defaults before validation, so a defaulted field can satisfy a
	// validate:"required" rule — matching the single-shot write ordering.
	if err := t.c.applyDefaults(t.ctx, obj); err != nil {
		return err
	}
	if err := t.c.validateStruct(t.ctx, obj); err != nil {
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

// DeleteEdge stages deletion of an edge from node uid over predicate. With no
// targetUIDs, every edge of that predicate is deleted; otherwise only the named
// target edges are removed.
func (t *TxnContext) DeleteEdge(uid, predicate string, targetUIDs ...string) error {
	if t.initErr != nil {
		return t.initErr
	}
	return t.txn.DeleteEdge(uid, predicate, targetUIDs...)
}

// DeleteNode stages deletion of one or more nodes by UID, removing all of their
// predicates.
func (t *TxnContext) DeleteNode(uids ...string) error {
	if t.initErr != nil {
		return t.initErr
	}
	return t.txn.DeleteNode(uids...)
}

// DeletePredicate stages deletion of every value of predicate on node uid,
// emitting the n-quad `<uid> <predicate> * .`. It clears a scalar predicate's
// value as well as all edges of that predicate on the node.
func (t *TxnContext) DeletePredicate(uid, predicate string) error {
	if t.initErr != nil {
		return t.initErr
	}
	// DeleteEdge with no target UIDs emits `<uid> <predicate> * .`, the n-quad
	// that deletes all values of a predicate — scalar or edge — on the node.
	return t.txn.DeleteEdge(uid, predicate)
}

// Commit commits the transaction's staged mutations and returns the pooled
// connection. After Commit the TxnContext must not be reused.
func (t *TxnContext) Commit() error {
	if t.initErr != nil {
		return t.initErr
	}
	err := t.txn.Commit()
	t.release()
	return err
}

// Discard abandons the transaction's staged mutations and returns the pooled
// connection. It is safe to call after Commit (a no-op) and is the correct
// deferred cleanup for every path.
func (t *TxnContext) Discard() {
	if t.txn != nil {
		// The underlying dgo transaction tracks its finished state, so Discard
		// after a successful Commit is a no-op; any error here is not actionable.
		_ = t.txn.Discard()
	}
	t.release()
}

// release returns the pooled connection to the pool exactly once, guarding
// against the Commit-then-deferred-Discard double call.
func (t *TxnContext) release() {
	if t.closed {
		return
	}
	t.closed = true
	if t.conn != nil {
		t.c.pool.put(t.conn)
	}
}
