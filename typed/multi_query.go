/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package typed

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/dgraph-io/dgdao"
	"github.com/dolan-in/dgman/v2"
)

// MultiQuery batches N homogeneous-type Query[T] blocks into a single
// Dgraph multi-block request. All blocks return rows of the same T; the
// per-block result is keyed by the block name supplied at Add.
//
// Dgraph executes the blocks concurrently on the server side; the entire
// batch costs one gRPC round-trip.
type MultiQuery[T any] struct {
	conn   dgdao.Client
	names  []string
	blocks map[string]*Query[T]
}

// NewMultiQuery constructs a MultiQuery bound to conn.
func NewMultiQuery[T any](conn dgdao.Client) *MultiQuery[T] {
	return &MultiQuery[T]{
		conn:   conn,
		blocks: make(map[string]*Query[T]),
	}
}

// Add registers a named block. The name must be a legal DQL block alias, names
// must be unique within one MultiQuery, and each *Query[T] may be added only
// once: Execute names the block's underlying dgman query in place, so
// registering the same Query pointer under two names would make both blocks
// render with whichever name was applied last. All three are programming errors
// and panic rather than fail at runtime.
func (mq *MultiQuery[T]) Add(name string, q *Query[T]) *MultiQuery[T] {
	if !validBlockName(name) {
		panic(fmt.Sprintf(
			"multi_query: invalid block name %q; must be a non-empty identifier of ASCII "+
				"letters, digits, and underscores, not starting with a digit", name))
	}
	if _, exists := mq.blocks[name]; exists {
		panic(fmt.Sprintf("multi_query: duplicate block name %q", name))
	}
	for existingName, existing := range mq.blocks {
		if existing == q {
			panic(fmt.Sprintf("multi_query: Query already added as %q; build a separate Query per block", existingName))
		}
	}
	mq.names = append(mq.names, name)
	mq.blocks[name] = q
	return mq
}

// BlockNames returns the registered block names in insertion order.
func (mq *MultiQuery[T]) BlockNames() []string {
	out := make([]string, len(mq.names))
	copy(out, mq.names)
	return out
}

// Execute runs every registered block in a single Dgraph round-trip and
// returns the per-block results, keyed by the block name supplied at Add.
// A block that matched no rows appears as an empty (non-nil) slice in the
// result map; the key is always present.
//
// Execute rejects blocks that carry WhereEdge constraints — those require a
// runtime pre-pass that cannot be folded into the multi-block batch. Run such
// queries individually with Query.Nodes.
//
// Dgraph keys response JSON by predicate name (e.g. resourceName), but Go
// structs typically use their json tag (e.g. name). Execute remaps the keys
// per T's tags before decoding — recursing into nested edge structs — so a
// schema that uses `dgraph:"predicate=..."` with a divergent `json:"..."`
// decodes correctly at every depth, matching dgman's QueryBlock.Scan path.
func (mq *MultiQuery[T]) Execute(ctx context.Context) (map[string][]T, error) {
	if len(mq.names) == 0 {
		return map[string][]T{}, nil
	}

	rawBlocks := make([]*dgman.Query, 0, len(mq.names))
	for _, name := range mq.names {
		block := mq.blocks[name]
		if len(block.edges) != 0 {
			return nil, fmt.Errorf(
				"multi_query: block %q carries WhereEdge constraints; "+
					"MultiQuery cannot batch edge-filtered blocks", name)
		}
		// Name the underlying dgman query so blocks do not collide on the
		// default "data" name and so the response JSON keys are predictable.
		block.q.Name(name)
		rawBlocks = append(rawBlocks, block.q)
	}

	dql := dgman.NewQueryBlock(rawBlocks...).String()
	raw, err := mq.conn.QueryRaw(ctx, dql, nil)
	if err != nil {
		return nil, fmt.Errorf("multi_query: dgraph: %w", err)
	}

	var perBlockRaw map[string]json.RawMessage
	if err := json.Unmarshal(raw, &perBlockRaw); err != nil {
		return nil, fmt.Errorf("multi_query: decoding response: %w", err)
	}

	rowType := reflect.TypeFor[T]()

	out := make(map[string][]T, len(mq.names))
	for _, name := range mq.names {
		body, ok := perBlockRaw[name]
		if !ok {
			out[name] = []T{}
			continue
		}
		remapped, err := remapPredicateKeys(body, rowType)
		if err != nil {
			return nil, fmt.Errorf("multi_query: remapping block %q: %w", name, err)
		}
		body = remapped
		var rows []T
		if err := json.Unmarshal(body, &rows); err != nil {
			return nil, fmt.Errorf("multi_query: decoding block %q: %w", name, err)
		}
		if rows == nil {
			rows = []T{}
		}
		out[name] = rows
	}
	return out, nil
}

// buildPredicateToJSONMap returns a map from dgraph predicate name → JSON tag
// name for fields on T where the two differ. Mirrors dgman's unexported helper
// of the same name; we need our own because the multi-block response from
// QueryRaw bypasses dgman's scan path.
func buildPredicateToJSONMap(t reflect.Type) map[string]string {
	t = getElemType(t)
	if t == nil || t.Kind() != reflect.Struct {
		return nil
	}
	result := make(map[string]string)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" || jsonTag == "-" {
			continue
		}
		jsonName := strings.Split(jsonTag, ",")[0]
		if jsonName == "" {
			continue
		}
		dgraphTag := field.Tag.Get("dgraph")
		if dgraphTag == "" {
			continue
		}
		var predName string
		for part := range strings.FieldsSeq(dgraphTag) {
			if p, ok := strings.CutPrefix(part, "predicate="); ok {
				predName = p
				break
			}
		}
		if predName == "" || predName == jsonName {
			continue
		}
		if predName == "uid" || predName == "dgraph.type" {
			continue
		}
		result[predName] = jsonName
	}
	return result
}

// remapPredicateKeys rewrites dgraph predicate names to JSON-tag names
// throughout a block body, descending into nested edge structs so a renamed
// predicate (dgraph `predicate=` diverging from `json=`) on a nested edge still
// decodes. It reproduces dgman's QueryBlock.Scan remap, which QueryRaw bypasses.
// The walk is type-driven by dstType (the row type T, or a nested field type),
// so only declared edge fields are recursed into; scalars and unrecognized keys
// pass through untouched. The top-level structural error is returned (so a
// malformed block surfaces its root cause); nested remaps are best-effort, since
// a type mismatch there surfaces with full context at the caller's typed decode.
func remapPredicateKeys(data json.RawMessage, dstType reflect.Type) (json.RawMessage, error) {
	dstType = getElemType(dstType)
	if dstType == nil || dstType.Kind() != reflect.Struct {
		return data, nil
	}
	switch firstNonSpace(data) {
	case '[':
		return remapArray(data, dstType)
	case '{':
		return remapObject(data, dstType)
	default:
		return data, nil
	}
}

// remapArray applies the per-object remap to every element of a JSON array whose
// elements decode into dstType.
func remapArray(data json.RawMessage, dstType reflect.Type) (json.RawMessage, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(data, &arr); err != nil {
		return nil, err
	}
	for i, item := range arr {
		if remapped, err := remapPredicateKeys(item, dstType); err == nil {
			arr[i] = remapped
		}
	}
	return json.Marshal(arr)
}

// remapObject renames dstType's renamed-predicate keys in one JSON object and
// recurses into its edge fields (struct- or slice-of-struct-typed).
func remapObject(data json.RawMessage, dstType reflect.Type) (json.RawMessage, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, err
	}
	predMap := buildPredicateToJSONMap(dstType)
	fieldTypes := buildFieldTypeMap(dstType)
	remapped := make(map[string]json.RawMessage, len(obj))
	for key, val := range obj {
		newKey := key
		if mapped, ok := predMap[key]; ok {
			newKey = mapped
		}
		if ft, ok := fieldTypes[newKey]; ok && getElemType(ft).Kind() == reflect.Struct {
			if rv, err := remapPredicateKeys(val, ft); err == nil {
				val = rv
			}
		}
		remapped[newKey] = val
	}
	return json.Marshal(remapped)
}

// buildFieldTypeMap maps each of t's JSON-tag names to its field type, so the
// remap can decide which keys are edges worth recursing into.
func buildFieldTypeMap(t reflect.Type) map[string]reflect.Type {
	t = getElemType(t)
	if t == nil || t.Kind() != reflect.Struct {
		return nil
	}
	result := make(map[string]reflect.Type, t.NumField())
	for i := range t.NumField() {
		field := t.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" || jsonTag == "-" {
			continue
		}
		jsonName := strings.Split(jsonTag, ",")[0]
		if jsonName == "" {
			continue
		}
		result[jsonName] = field.Type
	}
	return result
}

// getElemType unwraps pointer, slice, and array types to their base element
// type, so an edge field declared as *T, []T, or []*T resolves to T.
func getElemType(t reflect.Type) reflect.Type {
	for t != nil && (t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array) {
		t = t.Elem()
	}
	return t
}

// validBlockName reports whether name is a legal DQL query-block alias: a
// non-empty run of ASCII letters, digits, and underscores that does not start
// with a digit. Execute interpolates the name into the DQL block header (via
// Query.Name) and uses it as the response JSON key, so an unconstrained name —
// one with spaces, braces, parentheses, or other DQL punctuation — could
// corrupt the generated query or collide with the response structure. It also
// rejects reserved DQL keywords (var, query, mutation, schema) that are legal
// identifiers but would not denote a normal result block.
func validBlockName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c == '_',
			c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z':
			// always allowed
		case c >= '0' && c <= '9':
			if i == 0 {
				return false // a leading digit is not a legal identifier
			}
		default:
			return false
		}
	}
	// Reject reserved DQL block/operation keywords: they are legal identifiers
	// but would be interpreted as a special block (e.g. a `var` block) rather
	// than a normal result block.
	switch strings.ToLower(name) {
	case "var", "query", "mutation", "schema":
		return false
	}
	return true
}

// firstNonSpace returns the first non-whitespace byte of b, or 0 if none.
func firstNonSpace(b []byte) byte {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return c
		}
	}
	return 0
}
