/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package typed

import (
	"context"

	"github.com/dgraph-io/dgdao"
)

// Client provides type-safe CRUD and query operations over records of type T.
// T is the record struct (for example record.Actor); dgdao reflects over the
// struct's dgraph/json tags, so T needs no constraint.
//
// conn is the narrow dgdao.ClientCore data-ops surface, so one Client[T]
// serves both modes: bound to a connection client (NewClient) or scoped to a
// transaction (InTxn). Neither mode exposes connection lifecycle or
// transaction entry; start transactions on the untyped connection client's
// NewTxn.
type Client[T any] struct {
	conn dgdao.ClientCore
}

// NewClient binds a Client[T] to conn — the full connection client or a
// transaction-scoped *dgdao.ClientTxn.
func NewClient[T any](conn dgdao.ClientCore) *Client[T] {
	return &Client[T]{conn: conn}
}

// InTxn returns a Client[T] whose reads and writes run within tx. It binds
// the typed client to the transaction-scoped dgdao.InTxn(tx): typed writes
// (Insert, Update, Upsert, GetOrInsert, GetAndDelete, Delete) stage on tx and
// land on tx.Commit, and typed queries execute on tx's read-set.
//
// This is what makes a guarded read-then-delete correct across the typed
// layer. A Query built from the returned client runs its WhereEdge pre-pass
// through the same tx — the pre-pass reads through the client's conn, which
// is now tx — so the edge-match var block and the data block resolve against
// one transactional read-set. Were the pre-pass to read on a fresh
// connection, its read-set would split from the delete's, and a concurrent
// edge change could slip between the two without aborting the transaction.
func (c *Client[T]) InTxn(tx *dgdao.Txn) *Client[T] {
	return &Client[T]{conn: dgdao.InTxn(tx)}
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

// Insert inserts a new T. dgdao writes the assigned UID back into rec.
func (c *Client[T]) Insert(ctx context.Context, rec *T) (err error) {
	ctx, span := currentTracer().StartSpan(ctx, "insert", entityName[T]())
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

// GetOrInsert atomically gets the T matching the upsert predicates, or
// inserts rec when none exists, returning the resulting record and
// loaded=true when one already existed. Concurrent callers racing on the same
// key elect a single winner: exactly one inserts; the rest load the winner's
// record. Use it for idempotent creation — "ensure this record exists". With
// no predicates, the first field tagged dgraph:"upsert" is used.
// (cf. sync.Map.LoadOrStore)
func (c *Client[T]) GetOrInsert(ctx context.Context, rec *T, predicates ...string) (out *T, loaded bool, err error) {
	ctx, span := currentTracer().StartSpan(ctx, "getOrInsert", entityName[T]())
	defer func() { span.End(err) }()
	loaded, err = c.conn.GetOrInsert(ctx, rec, predicates...)
	if err != nil {
		return nil, false, err
	}
	return rec, loaded, nil
}

// GetAndDelete atomically reads the T whose key predicate equals key and
// deletes it, returning (nil, false, nil) when none matched. Concurrent
// callers racing on the same key elect a single winner: exactly one observes
// loaded=true. Use it to consume single-use records — OAuth2 state and
// authorization codes, JTI replay protection — where the read must also
// invalidate; compose it into an InTxn when the consume must commit together
// with other writes. With no predicates, the first field tagged
// dgraph:"upsert" is used. (cf. sync.Map.LoadAndDelete)
func (c *Client[T]) GetAndDelete(ctx context.Context, key any, predicates ...string) (rec *T, loaded bool, err error) {
	ctx, span := currentTracer().StartSpan(ctx, "getAndDelete", entityName[T]())
	defer func() { span.End(err) }()
	var out T
	loaded, err = c.conn.GetAndDelete(ctx, &out, key, predicates...)
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

// QueryRaw executes a raw DQL query with optional variables on the backing
// conn. On a transaction-scoped client the query reads within the
// transaction (read-your-writes).
func (c *Client[T]) QueryRaw(ctx context.Context, q string, vars map[string]string) (out []byte, err error) {
	ctx, span := currentTracer().StartSpan(ctx, "queryRaw", entityName[T]())
	defer func() { span.End(err) }()
	return c.conn.QueryRaw(ctx, q, vars)
}

// defaultPageSize is the page size Query.IterNodes uses to page through
// results.
const defaultPageSize = 50
