/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package filter_test

import (
	"strings"
	"testing"

	"github.com/dgraph-io/dgdao/typed/filter"
)

func TestAnyOfTextEmitsFilterAndBindsParam(t *testing.T) {
	b := &filter.Builder{}
	b.AnyOfText("resourceName", "honda civic")
	expr, params := b.Build()
	if !strings.Contains(expr, "anyoftext(resourceName, $1)") {
		t.Fatalf("expected anyoftext(resourceName, $1) in expr, got %q", expr)
	}
	if len(params) != 1 || params[0] != "honda civic" {
		t.Fatalf("expected params [\"honda civic\"], got %v", params)
	}
}

func TestAllOfTextEmitsFilterAndBindsParam(t *testing.T) {
	b := &filter.Builder{}
	b.AllOfText("description", "engine block")
	expr, params := b.Build()
	if !strings.Contains(expr, "alloftext(description, $1)") {
		t.Fatalf("expected alloftext(description, $1) in expr, got %q", expr)
	}
	if len(params) != 1 || params[0] != "engine block" {
		t.Fatalf("expected params [\"engine block\"], got %v", params)
	}
}

func TestAnyOfTextEmptyTermIsNoop(t *testing.T) {
	b := &filter.Builder{}
	b.AnyOfText("resourceName", "")
	expr, params := b.Build()
	if expr != "" || params != nil {
		t.Fatalf("expected empty expr/params for empty term, got %q / %v", expr, params)
	}
}
