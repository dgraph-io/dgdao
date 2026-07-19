/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"context"

	"github.com/dolan-in/dgman/v2"
)

// ClientCore is the record data-ops surface shared by the connection-scoped
// Client and the transaction-scoped *ClientTxn: exactly the operations that
// read or write records. Both satisfy it, so code generic over ClientCore —
// the typed and generated per-type clients — runs unchanged whether it is
// bound to a connection or scoped to a transaction.
//
// ClientCore deliberately excludes connection lifecycle (Close, WithRetry,
// schema and drop operations) and transaction entry (NewTxn, InTxn): a
// transaction-scoped value must not close the shared pool, retry by opening
// fresh transactions, or start a transaction within a transaction. Holding a
// ClientCore proves at the type level that none of those can occur.
type ClientCore interface {
	// Insert adds a new record or slice of records to the database.
	// The record must be a pointer to a struct with appropriate dgraph tags.
	Insert(context.Context, any) error

	// Upsert inserts a record if it doesn't exist or updates it if it does.
	// This operation requires a field with a unique directive in the dgraph
	// tag. If no predicates are specified, the first predicate with the
	// `upsert` tag will be used.
	Upsert(context.Context, any, ...string) error

	// GetOrInsert atomically gets the record matching the upsert predicates,
	// or inserts obj when none exists. It returns loaded=true when an
	// existing record matched (obj is then populated from it) and
	// loaded=false when obj was inserted. Concurrent callers racing on the
	// same key elect a single winner: exactly one inserts; the rest load the
	// winner's record. Use it for idempotent creation — "ensure this record
	// exists" — such as provisioning a per-subject record on first sight.
	// With no predicates, the first field tagged dgraph:"upsert" is used.
	// (cf. sync.Map.LoadOrStore)
	GetOrInsert(ctx context.Context, obj any, predicates ...string) (loaded bool, err error)

	// GetAndDelete atomically reads the record whose key predicate equals
	// key into obj and deletes it, returning loaded=false (and leaving obj
	// zero) when none matched. Concurrent callers racing on the same key
	// elect a single winner: exactly one observes loaded=true; every other
	// caller sees loaded=false. Use it to consume single-use records —
	// OAuth2 state and authorization codes, JTI replay protection — where
	// the read must also invalidate. With no predicates, the first field
	// tagged dgraph:"upsert" is used. (cf. sync.Map.LoadAndDelete)
	GetAndDelete(ctx context.Context, obj any, key any, predicates ...string) (loaded bool, err error)

	// Update modifies an existing record in the database.
	// The record must be a pointer to a struct and must have a UID field set.
	Update(context.Context, any) error

	// Get retrieves a single record by its UID and populates the provided
	// object. The object parameter must be a pointer to a struct.
	Get(context.Context, any, string) error

	// Query creates a new query builder for retrieving data from the
	// database. Returns a *dgman.Query that can be further refined with
	// filters, pagination, etc.
	Query(context.Context, any) *dgman.Query

	// QueryRaw executes a raw Dgraph query with optional query variables.
	// The `query` parameter is the Dgraph query string. The `vars` parameter
	// is a map of variable names to their values, used to parameterize the
	// query.
	QueryRaw(context.Context, string, map[string]string) ([]byte, error)

	// Delete removes records with the specified UIDs from the database.
	Delete(context.Context, []string) error
}

// Compile-time conformance: both the connection client and the
// transaction-scoped client satisfy ClientCore.
var (
	_ ClientCore = client{}
	_ ClientCore = (*ClientTxn)(nil)
)
