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

func (f *fakeRecord) RecordTypeName() string { return f.name }

type fakeWrapper struct{ inner *fakeRecord }

func (w *fakeWrapper) Record() *fakeRecord { return w.inner }

type fakeNonSchema struct{ X string }

func TestAsRecord_PassthroughForPlainStruct(t *testing.T) {
	in := &fakeNonSchema{X: "hi"}
	out := AsRecord(in)
	if out != any(in) {
		t.Fatalf("expected passthrough, got %T", out)
	}
}

func TestAsRecord_PassthroughForSchemaStruct(t *testing.T) {
	in := &fakeRecord{name: "Studio"}
	out := AsRecord(in)
	if out != any(in) {
		t.Fatalf("expected passthrough for direct Schema, got %T", out)
	}
}

func TestAsRecord_UnwrapsWrapper(t *testing.T) {
	inner := &fakeRecord{name: "Studio"}
	w := &fakeWrapper{inner: inner}
	out := AsRecord(w)
	if out != any(inner) {
		t.Fatalf("expected unwrapped inner, got %T (%v)", out, out)
	}
}

func TestAsRecord_IgnoresNonRecordRecordMethod(t *testing.T) {
	// A stray Record() method whose return value does not implement the Record
	// interface must not be mistaken for a dgdao entity: the secondary
	// interface check fails and the original value passes through.
	inner := errors.New("inner")
	outer := &recordErr{err: inner}
	out := AsRecord(outer)
	if out != any(outer) {
		t.Fatalf("expected passthrough for non-Record Record() method, got %T", out)
	}
}

type recordErr struct{ err error }

func (w *recordErr) Error() string { return w.err.Error() }
func (w *recordErr) Record() error { return w.err }

func TestAsRecord_NilInput(t *testing.T) {
	if out := AsRecord(nil); out != nil {
		t.Fatalf("expected nil for nil input, got %v", out)
	}
}

func TestAsRecord_TypedNilPointerDoesNotPanic(t *testing.T) {
	// fakeWrapper.Record dereferences its receiver, so invoking it on a typed
	// nil pointer would panic. AsRecord must return the value untouched.
	var w *fakeWrapper
	out := AsRecord(w)
	if out != any(w) {
		t.Fatalf("expected typed nil pointer passthrough, got %T (%v)", out, out)
	}
}

func TestAsRecord_PointerReceiverRecordOnValue(t *testing.T) {
	// fakeWrapper.Record has a pointer receiver. Passing the wrapper by value
	// must still unwrap: a value's method set excludes pointer-receiver methods,
	// so AsRecord looks Record up on an addressable copy.
	inner := &fakeRecord{name: "Studio"}
	w := fakeWrapper{inner: inner}
	out := AsRecord(w)
	if out != any(inner) {
		t.Fatalf("expected unwrapped inner from value wrapper, got %T (%v)", out, out)
	}
}

// recordingClient is the minimal surface needed to verify that wrappers
// passed to the Client interface get unwrapped before reaching internal
// reflection. It records whatever it received and returns nil. Each method
// applies obj = AsRecord(obj) at the top, mirroring the patch landing
// in this task.
type recordingClient struct {
	seen []any
}

func (c *recordingClient) capture(obj any) any {
	obj = AsRecord(obj)
	c.seen = append(c.seen, obj)
	return obj
}

func TestAsRecord_CaptureForwardsInner(t *testing.T) {
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

func TestAsRecord_CapturePassthroughForPlain(t *testing.T) {
	plain := &fakeNonSchema{X: "y"}
	c := &recordingClient{}
	got := c.capture(plain)
	if got != any(plain) {
		t.Fatalf("expected plain struct passthrough, got %T", got)
	}
}

func TestAsRecord_VariadicUnwrapsEachElement(t *testing.T) {
	innerA := &fakeRecord{name: "Studio"}
	innerB := &fakeRecord{name: "Film"}
	templates := []any{
		&fakeWrapper{inner: innerA},
		innerB, // already a Schema; passthrough
	}
	for i, obj := range templates {
		templates[i] = AsRecord(obj)
	}
	if templates[0] != any(innerA) {
		t.Fatalf("template[0]: expected innerA, got %T", templates[0])
	}
	if templates[1] != any(innerB) {
		t.Fatalf("template[1]: expected innerB (passthrough), got %T", templates[1])
	}
}

func TestAsRecord_SliceOfWrappersUnwrapsEach(t *testing.T) {
	// Insert/Upsert accept a slice of objects; a []*wrapper must be unwrapped
	// element-wise into the inner records, yielding a typed []T dgman accepts.
	a := &fakeRecord{name: "Studio"}
	b := &fakeRecord{name: "Film"}
	in := []*fakeWrapper{{inner: a}, {inner: b}}
	out := AsRecord(in)
	got, ok := out.([]*fakeRecord)
	if !ok {
		t.Fatalf("expected []*fakeRecord, got %T", out)
	}
	if len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("expected inner records [a b], got %v", got)
	}
}

func TestAsRecord_SliceOfPlainStructsUnchanged(t *testing.T) {
	// No element is a wrapper, so the original slice must come back untouched:
	// dgman writes generated UIDs back through this exact backing array, and a
	// rebuilt slice would break that for existing callers.
	in := []*fakeNonSchema{{X: "a"}, {X: "b"}}
	out := AsRecord(in)
	if reflect.ValueOf(out).Pointer() != reflect.ValueOf(in).Pointer() {
		t.Fatalf("expected the original slice, got a rebuilt %T", out)
	}
}

func TestAsRecord_EmptySlicePassthrough(t *testing.T) {
	in := []*fakeWrapper{}
	out := AsRecord(in)
	if reflect.ValueOf(out).Pointer() != reflect.ValueOf(in).Pointer() {
		t.Fatalf("expected the original empty slice, got %T", out)
	}
}

func TestAsRecord_MixedInnerTypesFallBackToAny(t *testing.T) {
	// A wrapper unwraps to *fakeRecord; a plain struct passes through as
	// *fakeNonSchema. The inner types differ, so the result is []any rather
	// than a typed slice, but every wrapper is still unwrapped.
	film := &fakeRecord{name: "Film"}
	w := &fakeWrapper{inner: film}
	rec := &fakeRecord{name: "Studio"}
	plain := &fakeNonSchema{X: "z"}
	in := []any{w, rec, plain}
	out := AsRecord(in)
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

func TestAsRecord_ArrayOfWrappersUnwrapsEach(t *testing.T) {
	a := &fakeRecord{name: "Studio"}
	b := &fakeRecord{name: "Film"}
	in := [2]*fakeWrapper{{inner: a}, {inner: b}}
	out := AsRecord(in)
	got, ok := out.([]*fakeRecord)
	if !ok {
		t.Fatalf("expected []*fakeRecord from array input, got %T", out)
	}
	if got[0] != a || got[1] != b {
		t.Fatalf("expected inner records [a b], got %v", got)
	}
}
