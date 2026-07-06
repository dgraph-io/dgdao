/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package filter_test

import (
	"testing"

	"github.com/dgraph-io/dgdao/typed/filter"
)

func TestParseUUID(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want filter.UUID
	}{
		{"plain", "abc", filter.UUID{Value: "abc"}},
		{"negated", "!abc", filter.UUID{Negated: true, Value: "abc"}},
		{"empty", "", filter.UUID{}},
		{"just bang", "!", filter.UUID{Negated: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filter.ParseUUID(tt.in)
			if got != tt.want {
				t.Errorf("ParseUUID(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseString(t *testing.T) {
	got := filter.ParseString("!hello")
	want := filter.String{Negated: true, Value: "hello"}
	if got != want {
		t.Errorf("ParseString = %+v, want %+v", got, want)
	}
}

func TestBuilder_Empty(t *testing.T) {
	var b filter.Builder
	expr, params := b.Build()
	if expr != "" || params != nil {
		t.Errorf("empty Build = (%q, %v), want (\"\", nil)", expr, params)
	}
}

func TestBuilder_EqGroupUUID_SingleTerm(t *testing.T) {
	var b filter.Builder
	b.EqGroupUUID("id", []filter.UUID{{Value: "u1"}})
	expr, params := b.Build()
	want := "(eq(id, $1))"
	if expr != want {
		t.Errorf("expr = %q, want %q", expr, want)
	}
	if len(params) != 1 || params[0] != "u1" {
		t.Errorf("params = %v, want [u1]", params)
	}
}

func TestBuilder_EqGroupUUID_MultipleTermsJoinWithOR(t *testing.T) {
	var b filter.Builder
	b.EqGroupUUID("id", []filter.UUID{{Value: "u1"}, {Value: "u2"}, {Negated: true, Value: "u3"}})
	expr, params := b.Build()
	want := "(eq(id, $1) OR eq(id, $2) OR NOT eq(id, $3))"
	if expr != want {
		t.Errorf("expr = %q, want %q", expr, want)
	}
	if len(params) != 3 {
		t.Errorf("len(params) = %d, want 3", len(params))
	}
}

func TestBuilder_EqGroupString_NoTermsIsNoop(t *testing.T) {
	var b filter.Builder
	b.EqGroupString("name", nil)
	expr, _ := b.Build()
	if expr != "" {
		t.Errorf("empty EqGroupString should be no-op, got expr=%q", expr)
	}
}

func TestBuilder_MultipleGroupsJoinWithAND(t *testing.T) {
	var b filter.Builder
	b.EqGroupUUID("id", []filter.UUID{{Value: "u1"}})
	b.EqGroupString("name", []filter.String{{Value: "Alice"}})
	expr, params := b.Build()
	want := "(eq(id, $1)) AND (eq(name, $2))"
	if expr != want {
		t.Errorf("expr = %q, want %q", expr, want)
	}
	if len(params) != 2 || params[0] != "u1" || params[1] != "Alice" {
		t.Errorf("params = %v, want [u1 Alice]", params)
	}
}

func TestBuilder_RequiredEqIsOwnGroup(t *testing.T) {
	var b filter.Builder
	b.RequiredEq("archiveStatus", "Active")
	b.EqGroupUUID("id", []filter.UUID{{Value: "u1"}})
	expr, params := b.Build()
	want := "eq(archiveStatus, $1) AND (eq(id, $2))"
	if expr != want {
		t.Errorf("expr = %q, want %q", expr, want)
	}
	if len(params) != 2 {
		t.Errorf("len(params) = %d, want 2", len(params))
	}
}

func TestBuilder_PositionalParamsAreSequential(t *testing.T) {
	var b filter.Builder
	b.EqGroupUUID("id", []filter.UUID{{Value: "a"}, {Value: "b"}})
	b.EqGroupString("name", []filter.String{{Value: "c"}})
	expr, params := b.Build()
	// Assert the exact expression: placeholders must be numbered $1..$N in
	// emission order and bound to the matching params. A substring check would
	// pass even if the numbering were scrambled (e.g. "$3 ... $1 ... $2").
	const want = "(eq(id, $1) OR eq(id, $2)) AND (eq(name, $3))"
	if expr != want {
		t.Errorf("expr = %q, want %q", expr, want)
	}
	wantParams := []any{"a", "b", "c"}
	if len(params) != len(wantParams) {
		t.Fatalf("params = %v, want %v", params, wantParams)
	}
	for i, p := range wantParams {
		if params[i] != p {
			t.Errorf("param[%d] = %v, want %v", i, params[i], p)
		}
	}
}
