/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"context"
	"encoding/json"
)

// Entity is the generic base embedded by generated entity types. It holds the
// backing record struct in an unexported field; the only access is the
// exported Record. Embedding Entity gives an entity type its Record, JSON
// marshaling, and validation for free.
type Entity[R any] struct {
	r *R
}

// AsEntity builds an Entity around r, adopting the record without copying it.
// Generated New<E>/New<E>WithRecord constructors use this to populate the
// embedded base.
func AsEntity[R any](r *R) Entity[R] {
	return Entity[R]{r: r}
}

// Record returns the backing record struct. dgdao uses this (via reflection —
// see AsRecord) to substitute the record struct when an entity crosses the
// client boundary.
func (e Entity[R]) Record() *R {
	return e.r
}

// MarshalJSON delegates to the record struct so its json tags drive output.
// The receiver is a pointer, so marshaling only engages through a pointer
// (*Agent, not Agent): a value-typed entity falls back to reflection over the
// unexported field and silently emits an empty object. Generated entities are
// always used as pointers.
func (e *Entity[R]) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.r)
}

// UnmarshalJSON lazily allocates the backing record if needed, then
// delegates. Safe to call on a zero-value Entity.
func (e *Entity[R]) UnmarshalJSON(data []byte) error {
	if e.r == nil {
		e.r = new(R)
	}
	return json.Unmarshal(data, e.r)
}

// Validate runs v against the backing record struct. v is satisfied by
// *github.com/go-playground/validator/v10.Validate.
func (e *Entity[R]) Validate(ctx context.Context, v StructValidator) error {
	return v.StructCtx(ctx, e.r)
}
