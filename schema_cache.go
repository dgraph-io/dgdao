/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"reflect"
	"sync"
)

// schemaCheckCache remembers which node types have already passed the
// per-write schema step in process(), so heavy write workloads don't pay a
// schema round-trip on every mutation:
//
//   - with AutoSchema enabled, the Go types already pushed via UpdateSchema
//   - with AutoSchema disabled, the Dgraph type names confirmed to exist in
//     the database schema
//
// Like consumeMu, it is held by pointer so every value copy of a client (and
// every NewClient call that dedupes to the same underlying client) shares one
// cache. DropAll invalidates it; schema Alters are additive in Dgraph, so
// UpdateSchema and AlterSchema do not. The deliberate trade-off is that
// schema changes made by *other* processes are not observed for types already
// cached — WithSchemaCache(false) restores the uncached per-write behavior.
type schemaCheckCache struct {
	mu       sync.Mutex
	synced   map[reflect.Type]struct{}
	verified map[string]struct{}
}

func newSchemaCheckCache() *schemaCheckCache {
	return &schemaCheckCache{
		synced:   make(map[reflect.Type]struct{}),
		verified: make(map[string]struct{}),
	}
}

// isSynced reports whether UpdateSchema has already succeeded for t.
func (s *schemaCheckCache) isSynced(t reflect.Type) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.synced[t]
	return ok
}

func (s *schemaCheckCache) markSynced(t reflect.Type) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.synced[t] = struct{}{}
}

// isVerified reports whether typeName has already been confirmed present in
// the database schema.
func (s *schemaCheckCache) isVerified(typeName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.verified[typeName]
	return ok
}

func (s *schemaCheckCache) markVerified(typeName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.verified[typeName] = struct{}{}
}

// invalidate forgets everything, forcing the next write of each type to
// redo its schema sync or verification.
func (s *schemaCheckCache) invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.synced = make(map[reflect.Type]struct{})
	s.verified = make(map[string]struct{})
}
