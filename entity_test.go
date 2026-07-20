/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"context"
	"encoding/json"
	"testing"
)

// agentRecord is a generated-style record struct: json tags plus the
// RecordTypeName marker method dgdao-gen emits.
type agentRecord struct {
	UID  string `json:"uid,omitempty"`
	Name string `json:"name,omitempty"`
}

func (r *agentRecord) RecordTypeName() string { return "Agent" }

// agentEntity is the shape of a dgdao-gen entity type: it embeds Entity[R]
// and gains Record, JSON marshaling, and validation from it.
type agentEntity struct {
	Entity[agentRecord]
}

func TestAsEntity_AdoptsRecordWithoutCopying(t *testing.T) {
	rec := &agentRecord{Name: "adopted"}
	e := AsEntity(rec)
	if e.Record() != rec {
		t.Fatalf("Record() = %p, want the adopted pointer %p", e.Record(), rec)
	}
	// Mutations through the original pointer must be visible via Record().
	rec.Name = "mutated"
	if got := e.Record().Name; got != "mutated" {
		t.Fatalf("Record().Name = %q, want %q", got, "mutated")
	}
}

func TestEntity_ZeroValueRecordIsNil(t *testing.T) {
	var e Entity[agentRecord]
	if e.Record() != nil {
		t.Fatalf("zero-value Record() = %v, want nil", e.Record())
	}
}

func TestEntity_MarshalJSONThroughPointerEmitsRecord(t *testing.T) {
	e := &agentEntity{Entity: AsEntity(&agentRecord{UID: "0x1", Name: "smith"})}
	out, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"uid":"0x1","name":"smith"}`
	if string(out) != want {
		t.Fatalf("Marshal = %s, want %s", out, want)
	}
}

// Entity documents that MarshalJSON only engages through a pointer: a
// value-typed entity falls back to reflection over the unexported field and
// silently emits an empty object. Pin that pitfall so a change to it is
// deliberate.
func TestEntity_MarshalJSONOnValueEmitsEmptyObject(t *testing.T) {
	e := agentEntity{Entity: AsEntity(&agentRecord{UID: "0x1", Name: "smith"})}
	out, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(out) != "{}" {
		t.Fatalf("value Marshal = %s, want {} (the documented pointer-receiver fallback)", out)
	}
}

func TestEntity_MarshalJSONZeroValueEmitsNull(t *testing.T) {
	e := &agentEntity{}
	out, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(out) != "null" {
		t.Fatalf("zero-value Marshal = %s, want null", out)
	}
}

func TestEntity_UnmarshalJSONLazilyAllocates(t *testing.T) {
	var e agentEntity
	if err := json.Unmarshal([]byte(`{"uid":"0x2","name":"jones"}`), &e); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	rec := e.Record()
	if rec == nil {
		t.Fatal("Record() = nil after Unmarshal; want a lazily allocated record")
	}
	if rec.UID != "0x2" || rec.Name != "jones" {
		t.Fatalf("Record() = %+v, want uid=0x2 name=jones", rec)
	}
}

func TestEntity_UnmarshalJSONUpdatesAdoptedRecordInPlace(t *testing.T) {
	rec := &agentRecord{UID: "0x3", Name: "before"}
	e := &agentEntity{Entity: AsEntity(rec)}
	if err := json.Unmarshal([]byte(`{"name":"after"}`), e); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if e.Record() != rec {
		t.Fatalf("Unmarshal replaced the adopted record; want in-place update")
	}
	if rec.Name != "after" || rec.UID != "0x3" {
		t.Fatalf("record = %+v, want name overwritten and uid kept", rec)
	}
}

// capturingValidator captures what Entity.Validate hands to the
// StructValidator, so the test can assert it receives the backing record
// rather than the entity wrapper.
type capturingValidator struct{ seen any }

func (v *capturingValidator) StructCtx(_ context.Context, s any) error {
	v.seen = s
	return nil
}

func TestEntity_ValidateRunsAgainstRecord(t *testing.T) {
	rec := &agentRecord{Name: "validate-me"}
	e := &agentEntity{Entity: AsEntity(rec)}
	v := &capturingValidator{}
	if err := e.Validate(context.Background(), v); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if v.seen != any(rec) {
		t.Fatalf("validator saw %T (%v), want the backing record %p", v.seen, v.seen, rec)
	}
}

// AsRecord is the client-boundary bridge for entities: it probes the promoted
// Record() method and substitutes the backing record. This is the integration
// the Entity type exists for, so pin it directly against a generated-style
// entity rather than only against hand-rolled wrapper fakes.
func TestAsRecord_SubstitutesEntityBackingRecord(t *testing.T) {
	rec := &agentRecord{Name: "bridge"}
	e := &agentEntity{Entity: AsEntity(rec)}
	out := AsRecord(e)
	if out != any(rec) {
		t.Fatalf("AsRecord = %T (%v), want the backing *agentRecord", out, out)
	}
}

// A slice of entities must unwrap element-wise into a typed slice of records,
// matching the Insert/Upsert "object or slice of objects" contract.
func TestAsRecord_SubstitutesEntitySliceElementWise(t *testing.T) {
	a, b := &agentRecord{Name: "a"}, &agentRecord{Name: "b"}
	in := []*agentEntity{
		{Entity: AsEntity(a)},
		{Entity: AsEntity(b)},
	}
	got, ok := AsRecord(in).([]*agentRecord)
	if !ok {
		t.Fatalf("AsRecord = %T, want []*agentRecord", AsRecord(in))
	}
	if len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("AsRecord slice = %v, want [%p %p]", got, a, b)
	}
}
