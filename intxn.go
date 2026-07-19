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

// ClientTxn is the transaction-scoped client returned by InTxn: the curated
// record data-ops surface (ClientCore) whose reads and writes route through a
// caller-supplied Txn instead of a fresh read-only or commit-now transaction.
// It holds only the Txn — no embedded connection client — so connection
// lifecycle (Close, WithRetry, schema and drop operations) and transaction
// entry (NewTxn, InTxn) are not hidden but absent: starting a transaction
// from within a transaction is unrepresentable at the type level.
//
// Defaults, validation, and unique-error translation come from the client
// that created the Txn (reached via tx.c), so a staged write behaves exactly
// like a single-shot write minus the immediate commit.
type ClientTxn struct {
	tx *Txn
}

// InTxn returns the transaction-scoped client for tx. It is the only
// ClientTxn constructor: a ClientTxn can be built from a live *Txn and
// nothing else. Client.InTxn delegates here; the typed layer calls it
// directly.
func InTxn(tx *Txn) *ClientTxn {
	return &ClientTxn{tx: tx}
}

// InTxn implements Client. See the Client.InTxn interface documentation for
// the full contract. The scoped client is bound to the client that created
// tx, so its options (validator, max edge traversal, embedding provider)
// match that client regardless of which client value InTxn is called on.
func (c client) InTxn(tx *Txn) *ClientTxn {
	return InTxn(tx)
}

// Query reads within the transaction (read-your-writes over staged
// mutations), mirroring Client.Query. The transaction governs the read
// context; the ctx argument is accepted for interface parity. Returns nil if
// the transaction failed to acquire a connection.
func (tc *ClientTxn) Query(_ context.Context, model any) *dgman.Query {
	return tc.tx.query(model)
}

// QueryRaw runs a raw DQL query within the transaction, mirroring
// Client.QueryRaw. The query observes writes already staged on the same txn.
func (tc *ClientTxn) QueryRaw(ctx context.Context, q string, vars map[string]string) ([]byte, error) {
	return tc.tx.queryRaw(ctx, q, vars)
}

// Get reads a single object by UID within the transaction, mirroring
// Client.Get.
func (tc *ClientTxn) Get(ctx context.Context, obj any, uid string) error {
	return tc.tx.get(ctx, obj, uid)
}

// Insert stages an insert of obj on the transaction after applying defaults
// and validating, mirroring Client.Insert minus the immediate commit.
func (tc *ClientTxn) Insert(ctx context.Context, obj any) error {
	return tc.stage(ctx, obj, func(o any) ([]string, error) {
		return tc.tx.txn.MutateBasic(o)
	})
}

// Upsert stages an insert-or-update of obj on the transaction after applying
// defaults and validating, mirroring Client.Upsert. With no predicates, the
// first field tagged dgraph:"upsert" is used as the match key.
func (tc *ClientTxn) Upsert(ctx context.Context, obj any, predicates ...string) error {
	return tc.stage(ctx, obj, func(o any) ([]string, error) {
		return tc.tx.txn.Upsert(o, predicates...)
	})
}

// Update stages an update of obj on the transaction after applying defaults
// and validating, mirroring Client.Update. obj must carry a UID.
func (tc *ClientTxn) Update(ctx context.Context, obj any) error {
	return tc.stage(ctx, obj, func(o any) ([]string, error) {
		return tc.tx.txn.MutateBasic(o)
	})
}

// Delete stages deletion of the given UIDs on the transaction, mirroring
// Client.Delete minus the immediate commit.
func (tc *ClientTxn) Delete(_ context.Context, uids []string) error {
	if tc.tx.initErr != nil {
		return tc.tx.initErr
	}
	return tc.tx.txn.DeleteNode(uids...)
}

// GetOrInsert stages obj only if no node matches the upsert predicates,
// reporting loaded=true when an existing node already matched (obj is then
// hydrated from it). It mirrors Client.GetOrInsert — atomic get-or-create,
// single winner, for idempotent creation — but stages on the transaction:
// the insert lands on Commit rather than immediately.
// (cf. sync.Map.LoadOrStore)
func (tc *ClientTxn) GetOrInsert(ctx context.Context, obj any, predicates ...string) (loaded bool, err error) {
	if tc.tx.initErr != nil {
		return false, tc.tx.initErr
	}
	obj = AsRecord(obj)
	if err := checkPointer(obj); err != nil {
		return false, err
	}
	// Apply defaults before validation, so a defaulted field can satisfy a
	// validate:"required" rule — matching the single-shot ordering.
	if err := tc.tx.c.applyDefaults(ctx, obj); err != nil {
		return false, err
	}
	if err := tc.tx.c.validateStruct(ctx, obj); err != nil {
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

// GetAndDelete reads the node whose key predicate equals key into obj and
// stages its deletion, returning loaded=false (and leaving obj zero) when
// none matched. It mirrors Client.GetAndDelete — atomic read-and-consume,
// single winner, for single-use records (OAuth2 state and authorization
// codes, JTI replay protection) — but because the read and delete already
// run inside the caller's transaction, it neither commits nor retries: the
// enclosing transaction owns commit, and conflict resolution against a
// concurrent consumer happens when that transaction commits (a real Dgraph
// cluster aborts the loser). With no predicates, the first dgraph:"upsert"
// field is used. (cf. sync.Map.LoadAndDelete)
func (tc *ClientTxn) GetAndDelete(_ context.Context, obj any, key any, predicates ...string) (loaded bool, err error) {
	if tc.tx.initErr != nil {
		return false, tc.tx.initErr
	}
	obj = AsRecord(obj)
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
		return false, fmt.Errorf("GetAndDelete: no key predicate (pass one or tag a field dgraph:\"upsert\")")
	}
	// The key value is parameterized ($1), but the predicate name is
	// concatenated straight into the DQL filter; reject anything that is not a
	// plain Dgraph predicate identifier before it reaches the filter.
	if !isValidPredicateName(pred) {
		return false, fmt.Errorf("GetAndDelete: invalid key predicate %q (allowed: letters, digits, '_', '.', '-')", pred)
	}

	getErr := tc.tx.txn.Get(obj).
		Filter("eq("+pred+", $1)", key).
		All(tc.tx.c.options.maxEdgeTraversal).
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
		return false, fmt.Errorf("GetAndDelete: matched a node but read no UID; the model needs a string UID field")
	}
	if delErr := tc.tx.txn.DeleteNode(uid); delErr != nil {
		return false, delErr
	}
	return true, nil
}

// stage runs the shared pre-mutation pipeline — entity unwrap, default
// population, struct validation — then the staged dgman write, and translates
// a Dgraph unique-constraint violation into a typed *UniqueError. It mirrors
// the pre-mutation steps of the single-shot Insert/Upsert/Update methods, so
// a staged write behaves exactly like a single-shot write minus the immediate
// commit.
//
// One exception: it does not run the single-shot methods' post-mutation
// injectShadowVectors step, so a dgraph:"embedding" SimString field staged
// here is written without its shadow __vec predicate.
func (tc *ClientTxn) stage(ctx context.Context, obj any, write func(any) ([]string, error)) error {
	if tc.tx.initErr != nil {
		return tc.tx.initErr
	}
	obj = AsRecord(obj)
	if err := tc.tx.c.applyDefaults(ctx, obj); err != nil {
		return err
	}
	if err := tc.tx.c.validateStruct(ctx, obj); err != nil {
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
