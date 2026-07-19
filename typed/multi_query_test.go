/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package typed_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dgraph-io/dgdao/typed"
)

// TestMultiQueryAddValidatesBlockName checks that Add accepts legal DQL block
// aliases and rejects names that would corrupt the generated DQL or the
// response keying. Name validation happens before any connection use, so these
// cases need no live conn.
func TestMultiQueryAddValidatesBlockName(t *testing.T) {
	t.Run("accepts valid identifiers", func(t *testing.T) {
		mq := typed.NewMultiQuery[widget](nil)
		valid := []string{"a", "byName", "block_2", "_private", "X1"}
		for _, name := range valid {
			mq.Add(name, typed.NewDetachedQuery[widget]())
		}
		if got := len(mq.BlockNames()); got != len(valid) {
			t.Fatalf("BlockNames count = %d, want %d", got, len(valid))
		}
	})
	invalid := []string{
		"", " ", "has space", "1leading", "dash-name", "paren()", "dot.name",
		"var", "query", "mutation", "schema", "VAR",
	}
	for _, name := range invalid {
		t.Run(fmt.Sprintf("rejects %q", name), func(t *testing.T) {
			mq := typed.NewMultiQuery[widget](nil)
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for invalid block name %q", name)
				}
			}()
			mq.Add(name, typed.NewDetachedQuery[widget]())
		})
	}
}

func TestMultiQueryAddAccumulatesBlocks(t *testing.T) {
	conn := newConn(t)
	mq := typed.NewMultiQuery[widget](conn)
	q1 := typed.NewClient[widget](conn).Query(context.Background())
	q2 := typed.NewClient[widget](conn).Query(context.Background())
	mq.Add("byName", q1)
	mq.Add("byQty", q2)
	got := mq.BlockNames()
	if len(got) != 2 || got[0] != "byName" || got[1] != "byQty" {
		t.Fatalf("BlockNames = %v, want [byName, byQty]", got)
	}
}

func TestMultiQueryAddRejectsDuplicateName(t *testing.T) {
	conn := newConn(t)
	mq := typed.NewMultiQuery[widget](conn)
	q := typed.NewClient[widget](conn).Query(context.Background())
	mq.Add("byName", q)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate block name")
		}
	}()
	mq.Add("byName", q)
}

func TestMultiQueryAddRejectsSameQueryTwice(t *testing.T) {
	conn := newConn(t)
	mq := typed.NewMultiQuery[widget](conn)
	q := typed.NewClient[widget](conn).Query(context.Background())
	mq.Add("first", q)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when the same Query is added under two names")
		}
	}()
	// Execute names the block's underlying query in place, so reusing one Query
	// pointer would corrupt block composition; Add must reject it up front.
	mq.Add("second", q)
}

func TestMultiQueryExecuteReturnsPerBlockResults(t *testing.T) {
	ctx := context.Background()
	conn := newConn(t)
	c := typed.NewClient[widget](conn)

	for _, w := range []*widget{
		{Name: "sprocket", Qty: 1},
		{Name: "gear", Qty: 5},
		{Name: "bolt", Qty: 10},
	} {
		if err := c.Insert(ctx, w); err != nil {
			t.Fatalf("Add %s: %v", w.Name, err)
		}
	}

	mq := typed.NewMultiQuery[widget](conn)
	mq.Add("all", c.Query(ctx))
	mq.Add("filtered", c.Query(ctx).Filter("eq(name, $1)", "gear"))

	results, err := mq.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := len(results["all"]); got != 3 {
		t.Fatalf("results[all] has %d rows, want 3", got)
	}
	if got := len(results["filtered"]); got != 1 {
		t.Fatalf("results[filtered] has %d rows, want 1", got)
	}
	if results["filtered"][0].Name != "gear" {
		t.Fatalf("results[filtered][0].Name = %q, want gear", results["filtered"][0].Name)
	}
}

func TestMultiQueryExecuteEmptyReturnsEmptyMap(t *testing.T) {
	conn := newConn(t)
	mq := typed.NewMultiQuery[widget](conn)
	results, err := mq.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute on empty MultiQuery: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected empty map, got %v", results)
	}
}

// renamed exercises the predicate-vs-json-tag remap. Dgraph returns the
// "thingName" key (the predicate name) but the struct's JSON tag is
// "name"; MultiQuery.Execute must remap before unmarshaling so Name
// populates.
type renamed struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Name  string   `json:"name,omitempty" dgraph:"predicate=thingName index=hash,fulltext"`
	Qty   int      `json:"qty,omitempty" dgraph:"index=int"`
}

func TestMultiQueryExecuteRemapsPredicateKeys(t *testing.T) {
	ctx := context.Background()
	conn := newConn(t)
	c := typed.NewClient[renamed](conn)

	for _, w := range []*renamed{
		{Name: "alpha", Qty: 1},
		{Name: "beta", Qty: 2},
	} {
		if err := c.Insert(ctx, w); err != nil {
			t.Fatalf("Add %s: %v", w.Name, err)
		}
	}

	mq := typed.NewMultiQuery[renamed](conn)
	mq.Add("all", c.Query(ctx))
	results, err := mq.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	rows := results["all"]
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	for _, r := range rows {
		if r.Name == "" {
			t.Fatalf("Name not populated; multi-block response was not remapped from predicate key: %+v", r)
		}
	}
}

// nestedChild sits on a nested edge and carries a renamed predicate: dgraph
// names it "childLabel" but the struct tags it json:"label".
type nestedChild struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Label string   `json:"label,omitempty" dgraph:"predicate=childLabel index=exact"`
}

// nestedParent has no renamed top-level predicate, only a renamed one on its
// nested child. A top-level-only remap therefore skips the block entirely and
// leaves Children[i].Label empty; the recursive remap descends the edge.
type nestedParent struct {
	UID      string         `json:"uid,omitempty"`
	DType    []string       `json:"dgraph.type,omitempty"`
	Name     string         `json:"name,omitempty" dgraph:"index=exact"`
	Children []*nestedChild `json:"children,omitempty"`
}

func TestMultiQueryExecuteRemapsNestedPredicateKeys(t *testing.T) {
	ctx := context.Background()
	conn := newConn(t)
	c := typed.NewClient[nestedParent](conn)

	p := &nestedParent{Name: "root", Children: []*nestedChild{{Label: "leaf"}}}
	if err := c.Insert(ctx, p); err != nil {
		t.Fatalf("Add: %v", err)
	}

	mq := typed.NewMultiQuery[nestedParent](conn)
	mq.Add("all", c.Query(ctx))
	results, err := mq.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	rows := results["all"]
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if len(rows[0].Children) != 1 {
		t.Fatalf("Children = %d, want 1", len(rows[0].Children))
	}
	// With the old top-level-only remap this is empty: the nested "childLabel"
	// predicate key is never rewritten to the struct's json:"label".
	if got := rows[0].Children[0].Label; got != "leaf" {
		t.Fatalf("nested Children[0].Label = %q, want leaf; nested predicate key not remapped", got)
	}
}
