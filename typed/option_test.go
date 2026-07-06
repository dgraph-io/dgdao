/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package typed_test

import (
	"strings"
	"testing"

	"github.com/dgraph-io/dgdao/typed"
)

func TestApply_RunsOptionsInOrder(t *testing.T) {
	type rec struct{ trail []string }
	r := &rec{}

	typed.Apply(r,
		func(x *rec) { x.trail = append(x.trail, "a") },
		func(x *rec) { x.trail = append(x.trail, "b") },
		func(x *rec) { x.trail = append(x.trail, "c") },
	)

	if got := strings.Join(r.trail, ""); got != "abc" {
		t.Fatalf("Apply ran options as %q, want %q", got, "abc")
	}
}

func TestApply_NoOptionsIsNoop(t *testing.T) {
	type rec struct{ n int }
	r := &rec{n: 7}
	typed.Apply(r)
	if r.n != 7 {
		t.Fatalf("Apply with no options mutated target: n = %d, want 7", r.n)
	}
}
