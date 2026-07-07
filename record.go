/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import "reflect"

// Schema identifies a value as a record of a generated schema-defining type.
// dgdao-gen-emitted schema structs implement this via a generated
// SchemaTypeName() method that returns the canonical entity name
// (e.g. "Studio"). The interface is intentionally minimal — a single method
// returning a useful piece of metadata.
//
// Plain user structs (not emitted by dgdao-gen) do not implement Schema
// and are unaffected by the dgdao.Client routing it enables; they pass
// through to the existing reflection-based dgman pipeline exactly as before.
type Schema interface {
	SchemaTypeName() string
}

// UnwrapSchema returns the schema-defining record contained in obj. If obj
// is nil, it is returned as-is. If obj is already a Schema, it is returned
// as-is. If obj exposes an Unwrap() method whose return value satisfies
// Schema, that return is substituted. Otherwise obj is returned unchanged.
//
// This is the bridge between dgdao-gen-emitted wrapper types and the
// rest of dgdao.Client. It is purely additive: types that don't
// implement Schema and don't have an Unwrap() method (i.e. existing
// dgdao users' plain structs) pass through untouched.
//
// Note on errors.Unwrap overlap: Go's errors package uses Unwrap() error
// as the standard "give me the wrapped thing" method. UnwrapSchema's
// secondary check (the returned value must itself implement Schema) means
// an error wrapper is not mistaken for a dgdao wrapper — the
// reflection probe finds Unwrap(), calls it, gets an error, fails the
// Schema check, and returns the original obj.
func UnwrapSchema(obj any) any {
	if obj == nil {
		return obj
	}
	if _, ok := obj.(Schema); ok {
		return obj
	}
	v := reflect.ValueOf(obj)
	if !v.IsValid() {
		return obj
	}
	// Insert, InsertRaw, and Upsert accept "an object or slice of objects".
	// A slice or array of wrappers must be unwrapped element-wise: otherwise
	// the wrappers reach dgman, which reflects over them and fails with an
	// opaque "cannot set uid/" while persisting nothing. Map over the elements.
	if k := v.Kind(); k == reflect.Slice || k == reflect.Array {
		return unwrapSchemaSlice(v, obj)
	}
	// A typed nil pointer has a valid method set, but invoking Unwrap on a nil
	// receiver would panic if the method dereferences it. Leave it untouched.
	if v.Kind() == reflect.Pointer && v.IsNil() {
		return obj
	}
	m := v.MethodByName("Unwrap")
	if !m.IsValid() && v.Kind() != reflect.Pointer {
		// Unwrap may be declared with a pointer receiver while obj was passed by
		// value; a value's method set excludes pointer-receiver methods, so look
		// it up on an addressable copy.
		pv := reflect.New(v.Type())
		pv.Elem().Set(v)
		m = pv.MethodByName("Unwrap")
	}
	if !m.IsValid() {
		return obj
	}
	mt := m.Type()
	if mt.NumIn() != 0 || mt.NumOut() != 1 {
		return obj
	}
	inner := m.Call(nil)[0].Interface()
	if _, ok := inner.(Schema); ok {
		return inner
	}
	return obj
}

// unwrapSchemaSlice unwraps each element of a slice or array. It returns obj
// unchanged when no element is a wrapper, so existing callers passing slices
// of plain structs are unaffected — important because dgman writes generated
// UIDs back through the original backing array, which rebuilding would break.
//
// When wrappers are present it builds a fresh slice of inner records: a typed
// []T when every inner record shares one concrete type (the common batch case,
// which dgman handles exactly as a directly-passed slice), or []any when the
// inner types differ.
func unwrapSchemaSlice(v reflect.Value, obj any) any {
	n := v.Len()
	if n == 0 {
		return obj
	}
	unwrapped := make([]any, n)
	changed := false
	homogeneous := true
	var elemType reflect.Type
	for i := range n {
		e := v.Index(i).Interface()
		u := UnwrapSchema(e)
		unwrapped[i] = u
		ut := reflect.TypeOf(u)
		if ut != reflect.TypeOf(e) {
			changed = true
		}
		switch {
		case ut == nil:
			homogeneous = false
		case i == 0:
			elemType = ut
		case ut != elemType:
			homogeneous = false
		}
	}
	if !changed {
		return obj
	}
	if homogeneous && elemType != nil {
		out := reflect.MakeSlice(reflect.SliceOf(elemType), n, n)
		for i := range n {
			out.Index(i).Set(reflect.ValueOf(unwrapped[i]))
		}
		return out.Interface()
	}
	return unwrapped
}
