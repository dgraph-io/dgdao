/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"errors"
	"reflect"
	"testing"
)

type fakeRecord struct{ name string }

func (f *fakeRecord) SchemaTypeName() string { return f.name }

type fakeWrapper struct{ inner *fakeRecord }

func (w *fakeWrapper) Unwrap() *fakeRecord { return w.inner }

type fakeNonSchema struct{ X string }

func TestUnwrapSchema_PassthroughForPlainStruct(t *testing.T) {
	in := &fakeNonSchema{X: "hi"}
	out := UnwrapSchema(in)
	if out != any(in) {
		t.Fatalf("expected passthrough, got %T", out)
	}
}

func TestUnwrapSchema_PassthroughForSchemaStruct(t *testing.T) {
	in := &fakeRecord{name: "Studio"}
	out := UnwrapSchema(in)
	if out != any(in) {
		t.Fatalf("expected passthrough for direct Schema, got %T", out)
	}
}

func TestUnwrapSchema_UnwrapsWrapper(t *testing.T) {
	inner := &fakeRecord{name: "Studio"}
	w := &fakeWrapper{inner: inner}
	out := UnwrapSchema(w)
	if out != any(inner) {
		t.Fatalf("expected unwrapped inner, got %T (%v)", out, out)
	}
}

func TestUnwrapSchema_IgnoresErrorsUnwrap(t *testing.T) {
	// errors.New("x") has no Unwrap; wrap one to get something with Unwrap() error.
	inner := errors.New("inner")
	outer := &wrappedErr{err: inner}
	out := UnwrapSchema(outer)
	if out != any(outer) {
		t.Fatalf("expected passthrough for error wrapper, got %T", out)
	}
}

type wrappedErr struct{ err error }

func (w *wrappedErr) Error() string { return w.err.Error() }
func (w *wrappedErr) Unwrap() error { return w.err }

func TestUnwrapSchema_NilInput(t *testing.T) {
	if out := UnwrapSchema(nil); out != nil {
		t.Fatalf("expected nil for nil input, got %v", out)
	}
}

func TestUnwrapSchema_TypedNilPointerDoesNotPanic(t *testing.T) {
	// fakeWrapper.Unwrap dereferences its receiver, so invoking it on a typed
	// nil pointer would panic. UnwrapSchema must return the value untouched.
	var w *fakeWrapper
	out := UnwrapSchema(w)
	if out != any(w) {
		t.Fatalf("expected typed nil pointer passthrough, got %T (%v)", out, out)
	}
}

func TestUnwrapSchema_PointerReceiverUnwrapOnValue(t *testing.T) {
	// fakeWrapper.Unwrap has a pointer receiver. Passing the wrapper by value
	// must still unwrap: a value's method set excludes pointer-receiver methods,
	// so UnwrapSchema looks Unwrap up on an addressable copy.
	inner := &fakeRecord{name: "Studio"}
	w := fakeWrapper{inner: inner}
	out := UnwrapSchema(w)
	if out != any(inner) {
		t.Fatalf("expected unwrapped inner from value wrapper, got %T (%v)", out, out)
	}
}

// recordingClient is the minimal surface needed to verify that wrappers
// passed to the Client interface get unwrapped before reaching internal
// reflection. It records whatever it received and returns nil. Each method
// applies obj = UnwrapSchema(obj) at the top, mirroring the patch landing
// in this task.
type recordingClient struct {
	seen []any
}

func (c *recordingClient) capture(obj any) any {
	obj = UnwrapSchema(obj)
	c.seen = append(c.seen, obj)
	return obj
}

func TestUnwrapSchema_CaptureForwardsInner(t *testing.T) {
	inner := &fakeRecord{name: "Studio"}
	w := &fakeWrapper{inner: inner}
	c := &recordingClient{}
	got := c.capture(w)
	if got != any(inner) {
		t.Fatalf("expected inner record, got %T (%v)", got, got)
	}
	if len(c.seen) != 1 || c.seen[0] != any(inner) {
		t.Fatalf("expected recording to hold inner record, got %v", c.seen)
	}
}

func TestUnwrapSchema_CapturePassthroughForPlain(t *testing.T) {
	plain := &fakeNonSchema{X: "y"}
	c := &recordingClient{}
	got := c.capture(plain)
	if got != any(plain) {
		t.Fatalf("expected plain struct passthrough, got %T", got)
	}
}

func TestUnwrapSchema_VariadicUnwrapsEachElement(t *testing.T) {
	innerA := &fakeRecord{name: "Studio"}
	innerB := &fakeRecord{name: "Film"}
	templates := []any{
		&fakeWrapper{inner: innerA},
		innerB, // already a Schema; passthrough
	}
	for i, obj := range templates {
		templates[i] = UnwrapSchema(obj)
	}
	if templates[0] != any(innerA) {
		t.Fatalf("template[0]: expected innerA, got %T", templates[0])
	}
	if templates[1] != any(innerB) {
		t.Fatalf("template[1]: expected innerB (passthrough), got %T", templates[1])
	}
}

func TestUnwrapSchema_SliceOfWrappersUnwrapsEach(t *testing.T) {
	// Insert/Upsert accept a slice of objects; a []*wrapper must be unwrapped
	// element-wise into the inner records, yielding a typed []T dgman accepts.
	a := &fakeRecord{name: "Studio"}
	b := &fakeRecord{name: "Film"}
	in := []*fakeWrapper{{inner: a}, {inner: b}}
	out := UnwrapSchema(in)
	got, ok := out.([]*fakeRecord)
	if !ok {
		t.Fatalf("expected []*fakeRecord, got %T", out)
	}
	if len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("expected inner records [a b], got %v", got)
	}
}

func TestUnwrapSchema_SliceOfPlainStructsUnchanged(t *testing.T) {
	// No element is a wrapper, so the original slice must come back untouched:
	// dgman writes generated UIDs back through this exact backing array, and a
	// rebuilt slice would break that for existing callers.
	in := []*fakeNonSchema{{X: "a"}, {X: "b"}}
	out := UnwrapSchema(in)
	if reflect.ValueOf(out).Pointer() != reflect.ValueOf(in).Pointer() {
		t.Fatalf("expected the original slice, got a rebuilt %T", out)
	}
}

func TestUnwrapSchema_EmptySlicePassthrough(t *testing.T) {
	in := []*fakeWrapper{}
	out := UnwrapSchema(in)
	if reflect.ValueOf(out).Pointer() != reflect.ValueOf(in).Pointer() {
		t.Fatalf("expected the original empty slice, got %T", out)
	}
}

func TestUnwrapSchema_MixedInnerTypesFallBackToAny(t *testing.T) {
	// A wrapper unwraps to *fakeRecord; a plain struct passes through as
	// *fakeNonSchema. The inner types differ, so the result is []any rather
	// than a typed slice, but every wrapper is still unwrapped.
	film := &fakeRecord{name: "Film"}
	w := &fakeWrapper{inner: film}
	rec := &fakeRecord{name: "Studio"}
	plain := &fakeNonSchema{X: "z"}
	in := []any{w, rec, plain}
	out := UnwrapSchema(in)
	got, ok := out.([]any)
	if !ok {
		t.Fatalf("expected []any for mixed inner types, got %T", out)
	}
	if got[0] != any(film) {
		t.Fatalf("expected wrapper unwrapped at [0], got %T", got[0])
	}
	if got[1] != any(rec) || got[2] != any(plain) {
		t.Fatalf("expected passthrough at [1],[2], got %v %v", got[1], got[2])
	}
}

func TestUnwrapSchema_ArrayOfWrappersUnwrapsEach(t *testing.T) {
	a := &fakeRecord{name: "Studio"}
	b := &fakeRecord{name: "Film"}
	in := [2]*fakeWrapper{{inner: a}, {inner: b}}
	out := UnwrapSchema(in)
	got, ok := out.([]*fakeRecord)
	if !ok {
		t.Fatalf("expected []*fakeRecord from array input, got %T", out)
	}
	if got[0] != a || got[1] != b {
		t.Fatalf("expected inner records [a b], got %v", got)
	}
}
