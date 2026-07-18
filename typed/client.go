/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package typed

import (
	"context"
	"iter"

	"github.com/dgraph-io/dgdao"
)

// Client provides type-safe CRUD and query operations over records of type T.
// T is the schema struct (for example schema.Actor); dgdao reflects over
// the struct's dgraph/json tags, so T needs no constraint.
type Client[T any] struct {
	conn dgdao.Client
}

// NewClient binds a Client[T] to conn.
func NewClient[T any](conn dgdao.Client) *Client[T] {
	return &Client[T]{conn: conn}
}

// NewTxnContext opens a validated, deferred-commit read-write transaction on
// the underlying client, mirroring dgdao.Client.NewTxnContext. Pass the
// returned *dgdao.TxnContext to InTxn to scope this typed client to it:
//
//	tx := typedClient.NewTxnContext(ctx)
//	defer tx.Discard()
//	scoped := typedClient.InTxn(tx)
func (c *Client[T]) NewTxnContext(ctx context.Context) *dgdao.TxnContext {
	return c.conn.NewTxnContext(ctx)
}

// InTxn returns a Client[T] whose reads and writes run within tx. It binds the
// typed client to the txn-scoped dgdao.Client (conn.InTxn(tx)): typed writes
// (Add, Update, Upsert, LoadOrStore, LoadAndDelete, Delete) stage on tx and land
// on tx.Commit, and typed queries execute on tx's read-set.
//
// This is what makes a guarded read-then-delete correct across the typed layer.
// A Query built from the returned client runs its WhereEdge pre-pass through the
// same tx — the pre-pass reads through the client's conn, which is now tx — so
// the edge-match var block and the data block resolve against one transactional
// read-set. Were the pre-pass to read on a fresh connection, its read-set would
// split from the delete's, and a concurrent edge change could slip between the
// two without aborting the transaction.
func (c *Client[T]) InTxn(tx *dgdao.TxnContext) *Client[T] {
	return &Client[T]{conn: c.conn.InTxn(tx)}
}

// Get loads the T with the given UID.
func (c *Client[T]) Get(ctx context.Context, uid string) (rec *T, err error) {
	ctx, span := currentTracer().StartSpan(ctx, "get", entityName[T]())
	defer func() { span.End(err) }()
	var out T
	if err = c.conn.Get(ctx, &out, uid); err != nil {
		return nil, err
	}
	return &out, nil
}

// Add inserts a new T. dgdao writes the assigned UID back into rec.
func (c *Client[T]) Add(ctx context.Context, rec *T) (err error) {
	ctx, span := currentTracer().StartSpan(ctx, "add", entityName[T]())
	defer func() { span.End(err) }()
	return c.conn.Insert(ctx, rec)
}

// Update modifies an existing T (must have its UID set).
func (c *Client[T]) Update(ctx context.Context, rec *T) (err error) {
	ctx, span := currentTracer().StartSpan(ctx, "update", entityName[T]())
	defer func() { span.End(err) }()
	return c.conn.Update(ctx, rec)
}

// Upsert inserts or updates rec, matching against predicates. With no
// predicates, the first field tagged dgraph:"upsert" is used.
func (c *Client[T]) Upsert(ctx context.Context, rec *T, predicates ...string) (err error) {
	ctx, span := currentTracer().StartSpan(ctx, "upsert", entityName[T]())
	defer func() { span.End(err) }()
	return c.conn.Upsert(ctx, rec, predicates...)
}

// LoadOrStore stores rec only if no node matches the upsert predicates,
// returning the resulting record and loaded=true when one already existed.
// Insert-if-absent (compare sync.Map.LoadOrStore). With no predicates, the
// first field tagged dgraph:"upsert" is used.
func (c *Client[T]) LoadOrStore(ctx context.Context, rec *T, predicates ...string) (out *T, loaded bool, err error) {
	ctx, span := currentTracer().StartSpan(ctx, "loadOrStore", entityName[T]())
	defer func() { span.End(err) }()
	loaded, err = c.conn.LoadOrStore(ctx, rec, predicates...)
	if err != nil {
		return nil, false, err
	}
	return rec, loaded, nil
}

// LoadAndDelete atomically reads the T whose key predicate equals key and
// deletes it, returning (nil, false, nil) when none matched. Read-and-consume
// (compare sync.Map.LoadAndDelete). With no predicates, the first field tagged
// dgraph:"upsert" is used.
func (c *Client[T]) LoadAndDelete(ctx context.Context, key any, predicates ...string) (rec *T, loaded bool, err error) {
	ctx, span := currentTracer().StartSpan(ctx, "loadAndDelete", entityName[T]())
	defer func() { span.End(err) }()
	var out T
	loaded, err = c.conn.LoadAndDelete(ctx, &out, key, predicates...)
	if err != nil || !loaded {
		return nil, loaded, err
	}
	return &out, true, nil
}

// Delete removes the T with the given UID.
func (c *Client[T]) Delete(ctx context.Context, uid string) (err error) {
	ctx, span := currentTracer().StartSpan(ctx, "delete", entityName[T]())
	defer func() { span.End(err) }()
	return c.conn.Delete(ctx, []string{uid})
}

// Query returns a typed query builder for T. conn and ctx are carried so the
// builder can run a WhereEdge pre-pass (see Query.WhereEdge) if one is needed.
func (c *Client[T]) Query(ctx context.Context) *Query[T] {
	var z T
	return &Query[T]{q: c.conn.Query(ctx, &z), conn: c.conn, ctx: ctx}
}

// defaultPageSize is the page size IterNodes uses to page through results.
const defaultPageSize = 50

// Iter returns an iterator over every T, paging transparently so large result
// sets are not materialized at once. It yields each record in turn; on error
// it yields a final (nil, err) and stops. All pages execute against one
// read-only transaction, so the iteration reads a single consistent snapshot.
func (c *Client[T]) Iter(ctx context.Context) iter.Seq2[*T, error] {
	return c.Query(ctx).IterNodes()
}
