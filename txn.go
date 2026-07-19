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

// Txn is a deferred-commit read-write transaction handle.
//
// The single-shot Client write methods (Insert, Upsert, Update) each open a
// dgman transaction with SetCommitNow, committing per call, so a caller cannot
// group several mutations into one atomic commit. A Txn removes that
// limit: it opens one dgman transaction WITHOUT SetCommitNow, and any number of
// reads, staged mutations, and deletes run against it until Commit lands them
// together.
//
// A Txn carries the transaction's lifecycle (Commit, Discard) and its
// graph-primitive deletes (DeleteEdge, DeleteNode, DeletePredicate). Its reads
// (query, queryRaw, get) are internal; both reads and validated writes —
// Insert, Upsert, Update, Delete, GetOrInsert, GetAndDelete — run through the
// transaction-scoped ClientTxn that InTxn(tx) returns, the public untyped
// surface for both. That scoped client applies the same defaults, validation, and
// unique-error translation as the single-shot methods, so grouping mutations
// into one atomic commit costs no validation. The idiomatic pattern is:
//
//	tx := client.NewTxn(ctx)
//	defer tx.Discard()
//	sc := client.InTxn(tx)
//	if err := sc.Upsert(ctx, obj); err != nil { return err }
//	if err := tx.DeleteEdge(uid, "pred"); err != nil { return err }
//	return tx.Commit()
//
// A Txn holds a Dgraph connection from the client pool for its lifetime.
// The caller must release it by calling Commit or Discard exactly once; the
// idiomatic pattern defers Discard immediately after NewTxn, since
// Discard is a no-op after a successful Commit but still guarantees the
// connection is returned to the pool on error and panic paths.
//
// Schema note: unlike the single-shot write methods, staged writes do not run
// autoSchema schema creation. A type written only through a transaction must
// already have its schema applied — by a prior single-shot write of that type,
// or by an explicit schema migration.
type Txn struct {
	c   client
	ctx context.Context
	// txn is the underlying deferred-commit dgman transaction. It is nil only
	// when the pool failed to hand out a connection; initErr records that error.
	txn     *dgman.TxnContext
	conn    *dgo.Dgraph
	initErr error
	closed  bool
}

// NewTxn starts a validated, deferred-commit read-write transaction.
//
// It checks out a connection from the client pool and opens a dgman transaction
// without SetCommitNow, so staged mutations do not commit until Commit is
// called. The returned Txn is never nil: if the pool cannot supply a
// connection, the error is deferred and surfaces from the first write method or
// from Commit, keeping the constructor's signature free of an error return.
func (c client) NewTxn(ctx context.Context) *Txn {
	t := &Txn{c: c, ctx: ctx}
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

// query returns a query builder that reads within the transaction, so its
// results reflect writes already staged on the same txn (read-your-writes). It
// mirrors Client.Query but binds to the transaction rather than a fresh
// read-only one, joining the transaction's read-set. It returns nil if the
// transaction failed to acquire a connection.
//
// Unexported: the public untyped read path is the transaction-scoped ClientTxn
// that InTxn(tx) returns (its Query delegates here); Txn itself does
// not expose reads.
func (t *Txn) query(model any) *dgman.Query {
	if t.initErr != nil || t.txn == nil {
		return nil
	}
	model = AsRecord(model)
	return t.txn.Get(model).All(t.c.options.maxEdgeTraversal)
}

// queryRaw runs a raw DQL query with optional variables within the transaction,
// mirroring Client.QueryRaw but reading against the transaction's read-set so
// the query observes writes already staged on the same txn.
//
// Unexported: reached only through the transaction-scoped ClientTxn from InTxn(tx),
// whose QueryRaw delegates here.
func (t *Txn) queryRaw(ctx context.Context, q string, vars map[string]string) ([]byte, error) {
	if t.initErr != nil {
		return nil, t.initErr
	}
	resp, err := t.txn.Txn().QueryWithVars(ctx, q, vars)
	if err != nil {
		return nil, err
	}
	return resp.GetJson(), nil
}

// get reads a single object by UID within the transaction, mirroring
// Client.Get. obj must be a non-nil pointer to a struct.
//
// Unexported: reached only through the transaction-scoped ClientTxn from InTxn(tx),
// whose Get delegates here.
func (t *Txn) get(ctx context.Context, obj any, uid string) error {
	if t.initErr != nil {
		return t.initErr
	}
	obj = AsRecord(obj)
	if err := checkPointer(obj); err != nil {
		return err
	}
	return t.txn.Get(obj).UID(uid).All(t.c.options.maxEdgeTraversal).Node()
}

// DeleteEdge stages deletion of an edge from node uid over predicate. With no
// targetUIDs, every edge of that predicate is deleted; otherwise only the named
// target edges are removed.
func (t *Txn) DeleteEdge(uid, predicate string, targetUIDs ...string) error {
	if t.initErr != nil {
		return t.initErr
	}
	return t.txn.DeleteEdge(uid, predicate, targetUIDs...)
}

// DeleteNode stages deletion of one or more nodes by UID, removing all of their
// predicates.
func (t *Txn) DeleteNode(uids ...string) error {
	if t.initErr != nil {
		return t.initErr
	}
	return t.txn.DeleteNode(uids...)
}

// DeletePredicate stages deletion of every value of predicate on node uid,
// emitting the n-quad `<uid> <predicate> * .`. It clears a scalar predicate's
// value as well as all edges of that predicate on the node.
func (t *Txn) DeletePredicate(uid, predicate string) error {
	if t.initErr != nil {
		return t.initErr
	}
	// DeleteEdge with no target UIDs emits `<uid> <predicate> * .`, the n-quad
	// that deletes all values of a predicate — scalar or edge — on the node.
	return t.txn.DeleteEdge(uid, predicate)
}

// Commit commits the transaction's staged mutations and returns the pooled
// connection. After Commit the Txn must not be reused.
func (t *Txn) Commit() error {
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
func (t *Txn) Discard() {
	if t.txn != nil {
		// The underlying dgo transaction tracks its finished state, so Discard
		// after a successful Commit is a no-op; any error here is not actionable.
		_ = t.txn.Discard()
	}
	t.release()
}

// release returns the pooled connection to the pool exactly once, guarding
// against the Commit-then-deferred-Discard double call.
func (t *Txn) release() {
	if t.closed {
		return
	}
	t.closed = true
	if t.conn != nil {
		t.c.pool.put(t.conn)
	}
}
