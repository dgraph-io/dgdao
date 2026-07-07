/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package typed_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dgraph-io/dgdao"
	"github.com/dgraph-io/dgdao/typed"
	"github.com/dolan-in/dgman/v2"
	"github.com/go-logr/logr/funcr"
)

// TestQuery_DetachedTerminalsReturnError verifies that executing a detached
// query — the filter-capture target returned by NewDetachedQuery for OrGroup
// and WhereEdge, which has no connection — returns ErrDetachedQuery from every
// terminal instead of nil-panicking on the absent underlying dgman query.
func TestQuery_DetachedTerminalsReturnError(t *testing.T) {
	if _, err := typed.NewDetachedQuery[widget]().Nodes(); !errors.Is(err, typed.ErrDetachedQuery) {
		t.Errorf("Nodes() error = %v, want ErrDetachedQuery", err)
	}
	if _, err := typed.NewDetachedQuery[widget]().First(); !errors.Is(err, typed.ErrDetachedQuery) {
		t.Errorf("First() error = %v, want ErrDetachedQuery", err)
	}
	if _, _, err := typed.NewDetachedQuery[widget]().NodesAndCount(); !errors.Is(err, typed.ErrDetachedQuery) {
		t.Errorf("NodesAndCount() error = %v, want ErrDetachedQuery", err)
	}
	var iterErr error
	rows := 0
	for _, err := range typed.NewDetachedQuery[widget]().IterNodes() {
		rows++
		iterErr = err
	}
	if rows != 1 || !errors.Is(iterErr, typed.ErrDetachedQuery) {
		t.Errorf("IterNodes yielded %d (rec, err) pairs, last err = %v; want exactly 1 with ErrDetachedQuery", rows, iterErr)
	}
}

// newCountingConn builds a file-backed dgdao client exactly like newConn,
// but wires in a logr.Logger that counts dgman query executions. dgman logs
// every executed query at verbosity 3 with the message "execute query"; the
// returned *int is incremented once per such log line.
//
// dgman's logger is process-global, and dgdao allows only one live
// file-backed engine per process (see dgdao.ErrSingletonOnly). Each call
// uses a fresh t.TempDir() URI for data isolation. Tests that use
// newCountingConn must NOT call t.Parallel(): a second live client would hit
// the engine singleton, and parallel tests would also corrupt the shared
// query count.
func newCountingConn(t *testing.T, count *int) dgdao.Client {
	t.Helper()
	logger := funcr.New(func(_, args string) {
		// funcr renders the message into args as `"msg"="execute query"`.
		// Match that exact pair so unrelated dgman/pool log lines (which log
		// other messages, e.g. "executeQuery" for query blocks) are ignored.
		if strings.Contains(args, `"msg"="execute query"`) {
			*count++
		}
	}, funcr.Options{Verbosity: 3})
	conn, err := dgdao.NewClient("file://"+t.TempDir(),
		dgdao.WithAutoSchema(true), dgdao.WithLogger(logger))
	if err != nil {
		t.Fatalf("dgdao.NewClient: %v", err)
	}
	t.Cleanup(conn.Close)
	return conn
}

func TestQuery_NodesReturnsAll(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	for _, n := range []string{"a", "b", "c"} {
		if err := c.Add(ctx, &widget{Name: n}); err != nil {
			t.Fatalf("Add %s: %v", n, err)
		}
	}

	got, err := c.Query(ctx).Nodes()
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("Nodes returned %d records, want 3", len(got))
	}
}

func TestQuery_LimitCapsResults(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	for i := range 5 {
		if err := c.Add(ctx, &widget{Name: "w", Qty: i}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}

	got, err := c.Query(ctx).Limit(2).Nodes()
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Limit(2) returned %d records, want 2", len(got))
	}
}

func TestQuery_FirstReturnsAMatch(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	if err := c.Add(ctx, &widget{Name: "only", Qty: 7}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, err := c.Query(ctx).First()
	if err != nil {
		t.Fatalf("First: %v", err)
	}
	if got == nil || got.Name != "only" {
		t.Fatalf("First returned %+v, want Name=only", got)
	}
}

func TestQuery_FirstNoMatchReturnsNilNil(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	got, err := c.Query(ctx).First()
	if err != nil {
		t.Fatalf("First on empty: unexpected error %v", err)
	}
	if got != nil {
		t.Fatalf("First on empty returned %+v, want nil", got)
	}
}

func TestQuery_BuilderChainCompilesAndRuns(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	if err := c.Add(ctx, &widget{Name: "x", Qty: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Every builder method must return *Query[widget] so the chain stays typed.
	_, err := c.Query(ctx).
		OrderAsc("qty").
		Offset(0).
		Limit(10).
		Cascade().
		Nodes()
	if err != nil {
		t.Fatalf("builder chain Nodes: %v", err)
	}
}

func TestQuery_RawExposesUnderlyingBuilder(t *testing.T) {
	c := typed.NewClient[widget](newConn(t))
	if c.Query(context.Background()).Raw() == nil {
		t.Fatal("Raw() returned nil; expected the underlying *dgman.Query")
	}
}

func TestQuery_Filter(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	// Insert three widgets with distinct names.
	for _, name := range []string{"alpha", "beta", "gamma"} {
		if err := c.Add(ctx, &widget{Name: name}); err != nil {
			t.Fatalf("Add %s: %v", name, err)
		}
	}

	// Filter to exactly those whose name equals "beta" (index=exact allows eq()).
	got, err := c.Query(ctx).Filter(`eq(name, "beta")`).Nodes()
	if err != nil {
		t.Fatalf("Filter Nodes: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Filter returned %d records, want 1", len(got))
	}
	if got[0].Name != "beta" {
		t.Fatalf("Filter returned Name=%q, want beta", got[0].Name)
	}
}

func TestQuery_FilterAccumulatesWithAnd(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	// Three widgets; only "beta"/9 satisfies BOTH name=="beta" and qty>=5.
	for _, w := range []widget{
		{Name: "alpha", Qty: 9},
		{Name: "beta", Qty: 9},
		{Name: "beta", Qty: 1},
	} {
		if err := c.Add(ctx, &w); err != nil {
			t.Fatalf("Add %+v: %v", w, err)
		}
	}

	// Two Filter calls must AND together, not overwrite. With last-write-wins
	// only ge(qty, 5) survives and this returns the two qty>=5 rows instead of
	// the single AND match.
	got, err := c.Query(ctx).
		Filter(`eq(name, "beta")`).
		Filter(`ge(qty, "5")`).
		Nodes()
	if err != nil {
		t.Fatalf("Filter Nodes: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("two ANDed Filters returned %d records, want 1 (name==beta AND qty>=5)", len(got))
	}
	if got[0].Name != "beta" || got[0].Qty != 9 {
		t.Fatalf("got %+v, want Name=beta Qty=9", got[0])
	}
}

func TestQuery_CombinedFilterShiftsPlaceholders(t *testing.T) {
	q := typed.NewDetachedQuery[widget]()
	if expr, params := q.CombinedFilter(); expr != "" || params != nil {
		t.Fatalf("empty CombinedFilter = (%q, %v), want (\"\", nil)", expr, params)
	}
	q.Filter("eq(name, $1)", "a")
	q.Filter("eq(qty, $1)", 7)
	expr, params := q.CombinedFilter()
	const want = "(eq(name, $1)) AND (eq(qty, $2))"
	if expr != want {
		t.Fatalf("CombinedFilter expr = %q, want %q", expr, want)
	}
	if len(params) != 2 || params[0] != "a" || params[1] != 7 {
		t.Fatalf("CombinedFilter params = %v, want [a 7]", params)
	}
}

// TestQuery_CombinedFilterParenthesizesFragments pins the precedence guarantee:
// a fragment that contains OR must stay grouped when it is ANDed with another
// fragment. Without per-fragment parentheses the expression would render as
// "a OR b AND c", which dgraph parses as "a OR (b AND c)" — silently widening
// the result set.
func TestQuery_CombinedFilterParenthesizesFragments(t *testing.T) {
	q := typed.NewDetachedQuery[widget]()
	q.Filter(`eq(name, "alpha") OR eq(name, "beta")`)
	q.Filter(`ge(qty, "5")`)
	expr, _ := q.CombinedFilter()
	const want = `(eq(name, "alpha") OR eq(name, "beta")) AND (ge(qty, "5"))`
	if expr != want {
		t.Fatalf("CombinedFilter precedence: expr = %q, want %q", expr, want)
	}
}

func TestQuery_OrGroup(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	for _, w := range []widget{
		{Name: "alpha", Qty: 9},
		{Name: "beta", Qty: 9},
		{Name: "gamma", Qty: 1},
	} {
		if err := c.Add(ctx, &w); err != nil {
			t.Fatalf("Add %+v: %v", w, err)
		}
	}

	// name == "alpha" OR name == "gamma": two of three rows.
	got, err := c.Query(ctx).OrGroup(
		typed.NewDetachedQuery[widget]().Filter(`eq(name, "alpha")`),
		typed.NewDetachedQuery[widget]().Filter(`eq(name, "gamma")`),
	).Nodes()
	if err != nil {
		t.Fatalf("OrGroup Nodes: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("OrGroup(alpha, gamma) returned %d rows, want 2", len(got))
	}

	// AND-of-OR: qty>=5 AND (name==alpha OR name==gamma) → only alpha/9.
	got, err = c.Query(ctx).
		Filter(`ge(qty, "5")`).
		OrGroup(
			typed.NewDetachedQuery[widget]().Filter(`eq(name, "alpha")`),
			typed.NewDetachedQuery[widget]().Filter(`eq(name, "gamma")`),
		).Nodes()
	if err != nil {
		t.Fatalf("AND-of-OR Nodes: %v", err)
	}
	if len(got) != 1 || got[0].Name != "alpha" {
		t.Fatalf("qty>=5 AND (alpha OR gamma) returned %+v, want [alpha/9]", got)
	}
}

func TestQuery_OrderAscDesc(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	// Insert widgets with distinct Qty values in non-sorted order so a
	// stable natural ordering cannot hide a missing sort.
	qtys := []int{30, 10, 50, 20, 40}
	for i, q := range qtys {
		if err := c.Add(ctx, &widget{Name: "w", Qty: q}); err != nil {
			t.Fatalf("Add widget[%d]: %v", i, err)
		}
	}

	// Ascending.
	asc, err := c.Query(ctx).OrderAsc("qty").Nodes()
	if err != nil {
		t.Fatalf("OrderAsc Nodes: %v", err)
	}
	if len(asc) != len(qtys) {
		t.Fatalf("OrderAsc returned %d records, want %d", len(asc), len(qtys))
	}
	for i := range len(asc) - 1 {
		if asc[i].Qty > asc[i+1].Qty {
			t.Fatalf("OrderAsc: asc[%d].Qty=%d > asc[%d].Qty=%d; not ascending",
				i, asc[i].Qty, i+1, asc[i+1].Qty)
		}
	}

	// Descending.
	desc, err := c.Query(ctx).OrderDesc("qty").Nodes()
	if err != nil {
		t.Fatalf("OrderDesc Nodes: %v", err)
	}
	if len(desc) != len(qtys) {
		t.Fatalf("OrderDesc returned %d records, want %d", len(desc), len(qtys))
	}
	for i := range len(desc) - 1 {
		if desc[i].Qty < desc[i+1].Qty {
			t.Fatalf("OrderDesc: desc[%d].Qty=%d < desc[%d].Qty=%d; not descending",
				i, desc[i].Qty, i+1, desc[i+1].Qty)
		}
	}
}

func TestQuery_OffsetSkipsResults(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	// Five widgets with distinct, deliberately unsorted Qty values.
	qtys := []int{40, 10, 50, 20, 30}
	for i, q := range qtys {
		if err := c.Add(ctx, &widget{Name: "w", Qty: q}); err != nil {
			t.Fatalf("Add widget[%d]: %v", i, err)
		}
	}

	// Ordering ascending by qty gives 10,20,30,40,50; Offset(2) drops the
	// first two, so 3 rows remain and the first is the 3rd-smallest (30).
	got, err := c.Query(ctx).OrderAsc("qty").Offset(2).Nodes()
	if err != nil {
		t.Fatalf("Offset Nodes: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("OrderAsc.Offset(2) returned %d records, want 3", len(got))
	}
	if got[0].Qty != 30 {
		t.Fatalf("first row after Offset(2) has Qty=%d, want 30 (3rd-smallest)", got[0].Qty)
	}
}

func TestQuery_AfterCursor(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	for i := range 5 {
		if err := c.Add(ctx, &widget{Name: "w", Qty: i}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}

	// First pass: grab all rows so we can pick a non-last cursor UID.
	all, err := c.Query(ctx).Nodes()
	if err != nil {
		t.Fatalf("first Nodes: %v", err)
	}
	if len(all) < 3 {
		t.Fatalf("expected at least 3 widgets, got %d", len(all))
	}
	cursor := all[1].UID // a non-last row

	// After(cursor) uses default UID ordering to skip past the cursor node.
	got, err := c.Query(ctx).After(cursor).Nodes()
	if err != nil {
		t.Fatalf("After Nodes: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("After(cursor) returned no rows; expected the rows past the cursor")
	}
	for _, w := range got {
		if w.UID <= cursor {
			t.Fatalf("After(%s) returned UID %s, which is not strictly greater than the cursor",
				cursor, w.UID)
		}
	}
}

func TestQuery_CascadeDropsIncompleteNodes(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	// Widgets with Qty > 0 carry a qty predicate. Widgets with Qty left 0
	// have it omitted entirely (json tag is omitempty), so they have no qty
	// predicate at all.
	withQty := []int{5, 9, 13}
	for _, q := range withQty {
		if err := c.Add(ctx, &widget{Name: "has-qty", Qty: q}); err != nil {
			t.Fatalf("Add qty=%d: %v", q, err)
		}
	}
	for i := range 4 {
		if err := c.Add(ctx, &widget{Name: "no-qty"}); err != nil {
			t.Fatalf("Add no-qty[%d]: %v", i, err)
		}
	}

	// @cascade(qty) drops any node that lacks the qty predicate.
	got, err := c.Query(ctx).Cascade("qty").Nodes()
	if err != nil {
		t.Fatalf("Cascade Nodes: %v", err)
	}
	if len(got) != len(withQty) {
		t.Fatalf("Cascade(qty) returned %d records, want %d (only the qty-bearing widgets)",
			len(got), len(withQty))
	}
	for _, w := range got {
		if w.Qty == 0 {
			t.Fatalf("Cascade(qty) returned a widget with Qty=0 (no qty predicate): %+v", w)
		}
	}
}

func TestQuery_FilterOrderLimitOffsetCombined(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	// A known set: five "keep" widgets plus a "drop" widget the filter excludes.
	for _, q := range []int{50, 20, 40, 10, 30} {
		if err := c.Add(ctx, &widget{Name: "keep", Qty: q}); err != nil {
			t.Fatalf("Add keep qty=%d: %v", q, err)
		}
	}
	if err := c.Add(ctx, &widget{Name: "drop", Qty: 99}); err != nil {
		t.Fatalf("Add drop: %v", err)
	}

	// Filter to name=keep -> qtys {10,20,30,40,50}; OrderAsc -> sorted;
	// Offset(1) drops 10; Limit(2) keeps {20,30}.
	got, err := c.Query(ctx).
		Filter(`eq(name, "keep")`).
		OrderAsc("qty").
		Offset(1).
		Limit(2).
		Nodes()
	if err != nil {
		t.Fatalf("combined chain Nodes: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("combined chain returned %d records, want 2", len(got))
	}
	if got[0].Qty != 20 || got[1].Qty != 30 {
		t.Fatalf("combined chain window = [%d, %d], want [20, 30]", got[0].Qty, got[1].Qty)
	}
}

func TestQuery_FirstOnMultipleRows(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	for _, q := range []int{30, 10, 20} {
		if err := c.Add(ctx, &widget{Name: "w", Qty: q}); err != nil {
			t.Fatalf("Add qty=%d: %v", q, err)
		}
	}

	// First on an ascending-by-qty query yields exactly the smallest row.
	got, err := c.Query(ctx).OrderAsc("qty").First()
	if err != nil {
		t.Fatalf("First: %v", err)
	}
	if got == nil {
		t.Fatal("First returned nil on a non-empty result set")
	}
	if got.Qty != 10 {
		t.Fatalf("First on OrderAsc(qty) returned Qty=%d, want 10 (smallest)", got.Qty)
	}
}

func TestQuery_NodesEmptyResult(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t)) // fresh client, no inserts

	got, err := c.Query(ctx).Nodes()
	if err != nil {
		t.Fatalf("Nodes on empty client: unexpected error %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Nodes on empty client returned %d records, want 0", len(got))
	}
}

func TestQuery_OrderAccumulates(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	// OrderAsc and OrderDesc accumulate: both clauses must survive on the
	// same query. dgman renders them as "orderasc:"/"orderdesc:" in the
	// generated query string.
	q := c.Query(ctx).OrderAsc("name").OrderDesc("qty")
	s := q.Raw().String()
	if !strings.Contains(s, "orderasc: name") {
		t.Fatalf("query string missing ascending name order; got:\n%s", s)
	}
	if !strings.Contains(s, "orderdesc: qty") {
		t.Fatalf("query string missing descending qty order; got:\n%s", s)
	}
}

func TestQuery_CascadeOverwrites(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	// Cascade overwrites: the second call wins, the first predicate is gone.
	// dgman renders predicates as @cascade(pred1,pred2,...) with no spaces.
	q := c.Query(ctx).Cascade("name").Cascade("qty")
	s := q.Raw().String()
	if !strings.Contains(s, "@cascade(qty)") {
		t.Fatalf("second Cascade(qty) not rendered in query string; got:\n%s", s)
	}
	if strings.Contains(s, "@cascade(name)") {
		t.Fatalf("first Cascade(name) still present after overwrite; got:\n%s", s)
	}
}

func TestQuery_TerminalRunsTwice(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	for _, n := range []string{"a", "b", "c"} {
		if err := c.Add(ctx, &widget{Name: n}); err != nil {
			t.Fatalf("Add %s: %v", n, err)
		}
	}

	// A terminal is re-runnable: calling Nodes twice on the same builder
	// succeeds both times and yields equal-length results.
	q := c.Query(ctx)
	first, err := q.Nodes()
	if err != nil {
		t.Fatalf("first Nodes: %v", err)
	}
	second, err := q.Nodes()
	if err != nil {
		t.Fatalf("second Nodes: %v", err)
	}
	if len(first) != len(second) {
		t.Fatalf("Nodes run twice returned %d then %d records; want equal lengths",
			len(first), len(second))
	}
}

func TestQuery_BuilderAliasesAndAccumulates(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	// (i) Filter accumulates: after two Filter calls both survive, ANDed.
	q := c.Query(ctx)
	q.Filter(`eq(name, "alpha")`)
	q.Filter(`eq(name, "beta")`)
	s := q.Raw().String()
	if !strings.Contains(s, `eq(name, "alpha")`) {
		t.Fatalf("Filter A dropped; want both fragments present in:\n%s", s)
	}
	if !strings.Contains(s, `eq(name, "beta")`) {
		t.Fatalf("Filter B dropped; want both fragments present in:\n%s", s)
	}
	if !strings.Contains(s, " AND ") {
		t.Fatalf("accumulated filters not ANDed; got:\n%s", s)
	}

	// (ii) The builder aliases: a saved reference and further mutation observe
	// the same underlying query. ref and q point at the same *Query, so a
	// mutation through one is visible through the other. This documents the
	// single-use footgun: you cannot branch a saved builder.
	ref := q
	if ref != q {
		t.Fatal("builder reference is not identical to the original *Query")
	}
	q.OrderAsc("name")
	if ref.Raw().String() != q.Raw().String() {
		t.Fatal("mutating q did not affect ref; builder is expected to alias a shared query")
	}
	if !strings.Contains(ref.Raw().String(), "orderasc: name") {
		t.Fatalf("order applied via q not visible through ref; got:\n%s", ref.Raw().String())
	}
}

func TestQuery_RawRoundTrips(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	if err := c.Add(ctx, &widget{Name: "raw-target", Qty: 7}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Take the raw *dgman.Query, apply a dgman-only builder method directly,
	// then execute via the raw query's own Nodes(&dst).
	var raw *dgman.Query = c.Query(ctx).Raw()
	raw.OrderAsc("qty")

	var dst []widget
	if err := raw.Nodes(&dst); err != nil {
		t.Fatalf("raw query Nodes: %v", err)
	}
	if len(dst) != 1 {
		t.Fatalf("raw query returned %d records, want 1", len(dst))
	}
	if dst[0].Name != "raw-target" || dst[0].Qty != 7 {
		t.Fatalf("raw query returned %+v, want Name=raw-target Qty=7", dst[0])
	}
}

func TestQuery_SingleQueryPerTerminal(t *testing.T) {
	// Uses the global dgman logger; must not run in parallel.
	ctx := context.Background()
	// queriesExecuted is incremented by newCountingConn's logger each time
	// dgman runs a query, so it reflects real database round-trips.
	var queriesExecuted int
	c := typed.NewClient[widget](newCountingConn(t, &queriesExecuted))

	for i := range 2 {
		if err := c.Add(ctx, &widget{Name: "w", Qty: i}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}

	// Building the chain runs no queries: builder methods only mutate the AST.
	before := queriesExecuted
	q := c.Query(ctx).Filter(`eq(name, "w")`).OrderAsc("qty").Limit(10)
	if queriesExecuted != before {
		t.Fatalf("builder methods executed %d queries, want 0", queriesExecuted-before)
	}

	// The Nodes terminal runs exactly one query.
	if _, err := q.Nodes(); err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if got := queriesExecuted - before; got != 1 {
		t.Fatalf("Nodes executed %d queries, want exactly 1", got)
	}

	// A fresh builder's First terminal also runs exactly one query.
	before = queriesExecuted
	if _, err := c.Query(ctx).First(); err != nil {
		t.Fatalf("First: %v", err)
	}
	if got := queriesExecuted - before; got != 1 {
		t.Fatalf("First executed %d queries, want exactly 1", got)
	}
}

func TestIterNodes_StreamsAll(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	const n = 125 // > defaultPageSize (50): forces multiple pages
	for i := range n {
		if err := c.Add(ctx, &widget{Name: "w", Qty: i}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	seen := 0
	for w, err := range c.Query(ctx).IterNodes() {
		if err != nil {
			t.Fatalf("IterNodes yielded error: %v", err)
		}
		if w == nil {
			t.Fatal("IterNodes yielded a nil widget")
		}
		seen++
	}
	if seen != n {
		t.Fatalf("IterNodes streamed %d records, want %d", seen, n)
	}
}

func TestIterNodes_StopsOnConsumerBreak(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	const n = 125
	for i := range n {
		if err := c.Add(ctx, &widget{Name: "w", Qty: i}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	seen := 0
	for _, err := range c.Query(ctx).IterNodes() {
		if err != nil {
			t.Fatalf("IterNodes yielded error: %v", err)
		}
		seen++
		if seen == 10 {
			break
		}
	}
	if seen != 10 {
		t.Fatalf("IterNodes yielded %d records after break at 10, want 10", seen)
	}
}

func TestIterNodes_EmptyResult(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	seen := 0
	for _, err := range c.Query(ctx).IterNodes() {
		if err != nil {
			t.Fatalf("IterNodes over empty set yielded error: %v", err)
		}
		seen++
	}
	if seen != 0 {
		t.Fatalf("IterNodes over empty set yielded %d records, want 0", seen)
	}
}

func TestIterNodes_RespectsLimit(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	const n = 100
	for i := range n {
		if err := c.Add(ctx, &widget{Name: "w", Qty: i}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	seen := 0
	for _, err := range c.Query(ctx).Limit(30).IterNodes() {
		if err != nil {
			t.Fatalf("IterNodes yielded error: %v", err)
		}
		seen++
	}
	if seen != 30 {
		t.Fatalf("Limit(30).IterNodes() streamed %d records, want 30", seen)
	}
}

func TestIterNodes_LimitExceedsResultSet(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	const n = 30
	for i := range n {
		if err := c.Add(ctx, &widget{Name: "w", Qty: i}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	seen := 0
	for _, err := range c.Query(ctx).Limit(500).IterNodes() {
		if err != nil {
			t.Fatalf("IterNodes yielded error: %v", err)
		}
		seen++
	}
	if seen != n {
		t.Fatalf("Limit(500).IterNodes() over %d records streamed %d, want %d", n, seen, n)
	}
}

func TestIterNodes_RespectsOffset(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	// Qty values start at 1 (not 0) so omitempty never suppresses the field,
	// keeping OrderAsc("qty") a true total order over all records.
	const n = 10
	for i := range n {
		if err := c.Add(ctx, &widget{Name: "w", Qty: i + 1}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	got := make([]int, 0, n-3) // offset 3 of n records
	for w, err := range c.Query(ctx).OrderAsc("qty").Offset(3).IterNodes() {
		if err != nil {
			t.Fatalf("IterNodes yielded error: %v", err)
		}
		got = append(got, w.Qty)
	}
	if len(got) != 7 {
		t.Fatalf("Offset(3).IterNodes() streamed %d records, want 7", len(got))
	}
	for i, q := range got {
		if q != i+4 { // Qty=1..10; offset 3 skips 1,2,3 → starts at 4
			t.Fatalf("Offset(3).IterNodes()[%d] Qty = %d, want %d", i, q, i+4)
		}
	}
}

func TestIterNodes_RespectsOffsetAndLimit(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	// Qty values start at 1 so omitempty never suppresses the field and
	// OrderAsc("qty") is a strict total order across all 200 records.
	const n = 200
	for i := range n {
		if err := c.Add(ctx, &widget{Name: "w", Qty: i + 1}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	got := make([]int, 0, 120) // Limit(120)
	for w, err := range c.Query(ctx).OrderAsc("qty").Offset(60).Limit(120).IterNodes() {
		if err != nil {
			t.Fatalf("IterNodes yielded error: %v", err)
		}
		got = append(got, w.Qty)
	}
	if len(got) != 120 {
		t.Fatalf("Offset(60).Limit(120).IterNodes() streamed %d records, want 120", len(got))
	}
	for i, q := range got {
		if q != i+61 { // Qty=1..200; offset 60 skips 1..60 → starts at 61
			t.Fatalf("result[%d] Qty = %d, want %d", i, q, i+61)
		}
	}
}

func TestIterNodes_OneQueryPerPage(t *testing.T) {
	ctx := context.Background()
	var queriesExecuted int
	c := typed.NewClient[widget](newCountingConn(t, &queriesExecuted))
	const n = 125 // ceil(125/50) = 3 page queries
	for i := range n {
		if err := c.Add(ctx, &widget{Name: "w", Qty: i}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	// Obtaining the iterator runs no query — IterNodes is lazy. Measure the
	// delta around the build, not the absolute count, so the assertion holds
	// regardless of how many queries the seeding above happened to run.
	before := queriesExecuted
	seq := c.Query(ctx).IterNodes()
	if delta := queriesExecuted - before; delta != 0 {
		t.Fatalf("building the IterNodes iterator executed %d queries, want 0", delta)
	}
	seen := 0
	for _, err := range seq {
		if err != nil {
			t.Fatalf("IterNodes yielded error: %v", err)
		}
		seen++
	}
	if seen != n {
		t.Fatalf("IterNodes streamed %d records, want %d", seen, n)
	}
	if delta := queriesExecuted - before; delta != 3 { // ceil(125/50) = 3 pages
		t.Fatalf("IterNodes over %d records ran %d queries, want 3", n, delta)
	}
}

func TestIterNodes_YieldsErrorAndStops(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	if err := c.Add(ctx, &widget{Name: "w", Qty: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// A syntactically invalid @filter (unbalanced parenthesis) makes the page
	// query fail at execution; IterNodes must yield one (nil, err) and stop.
	gotErr := false
	for w, err := range c.Query(ctx).Filter(`eq(name, "w"`).IterNodes() {
		if err != nil {
			gotErr = true
			if w != nil {
				t.Fatalf("error yield carried a non-nil widget: %+v", w)
			}
			break
		}
		t.Fatal("IterNodes over a malformed query yielded a record before erroring")
	}
	if !gotErr {
		t.Fatal("IterNodes over a malformed query did not yield an error")
	}
}

func TestQuery_LimitOffsetStillDriveNodes(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	// Qty values start at 1 so omitempty never suppresses the field and
	// OrderAsc("qty") is a strict total order across all records.
	const n = 10
	for i := range n {
		if err := c.Add(ctx, &widget{Name: "w", Qty: i + 1}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	// Regression: Limit/Offset now also set Query struct fields; confirm they
	// still drive the Nodes terminal.
	got, err := c.Query(ctx).OrderAsc("qty").Offset(2).Limit(3).Nodes()
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("Offset(2).Limit(3).Nodes() returned %d records, want 3", len(got))
	}
	for i, w := range got {
		if w.Qty != i+3 { // Qty=1..10; offset 2 skips 1,2 → starts at 3
			t.Fatalf("Nodes()[%d] Qty = %d, want %d", i, w.Qty, i+3)
		}
	}
}

func TestQuery_RootFuncOverridesRoot(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	for _, n := range []string{"a", "b", "c"} {
		if err := c.Add(ctx, &widget{Name: n}); err != nil {
			t.Fatalf("Add %s: %v", n, err)
		}
	}
	// RootFunc replaces the default type(widget) root with an eq() lookup;
	// the query still decodes into []widget.
	got, err := c.Query(ctx).RootFunc(`eq(name, "b")`).Nodes()
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf(`RootFunc(eq(name,"b")).Nodes() returned %d records, want 1`, len(got))
	}
	if got[0].Name != "b" {
		t.Fatalf("RootFunc lookup returned %q, want \"b\"", got[0].Name)
	}
}

func TestQuery_RootFuncRendersAndOverwrites(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	// RootFunc renders into the (func: ...) position and overwrites: the
	// second call wins.
	q := c.Query(ctx).RootFunc(`eq(name, "x")`).RootFunc(`eq(name, "y")`)
	s := q.Raw().String()
	if !strings.Contains(s, `func: eq(name, "y")`) {
		t.Fatalf("second RootFunc not rendered; got:\n%s", s)
	}
	if strings.Contains(s, `eq(name, "x")`) {
		t.Fatalf("first RootFunc still present after overwrite; got:\n%s", s)
	}
}

func TestQuery_NameDecodesAfterRename(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	for _, n := range []string{"a", "b", "c"} {
		if err := c.Add(ctx, &widget{Name: n}); err != nil {
			t.Fatalf("Add %s: %v", n, err)
		}
	}
	// Name renames the query block. dgman uses the name symmetrically to
	// generate and decode, so a renamed block still decodes into []widget.
	got, err := c.Query(ctx).Name("widgets").Nodes()
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf(`Name("widgets").Nodes() returned %d records, want 3`, len(got))
	}
}

func TestQuery_NameRendersAndOverwrites(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	// Name renders as the block name and overwrites: the second call wins.
	q := c.Query(ctx).Name("first").Name("second")
	s := q.Raw().String()
	if !strings.Contains(s, "second(func:") {
		t.Fatalf("second Name not rendered as block name; got:\n%s", s)
	}
	if strings.Contains(s, "first(func:") {
		t.Fatalf("first Name still present after overwrite; got:\n%s", s)
	}
}

func TestQuery_AsRendersAndOverwrites(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	// As transitions to *RawQuery, prefixes the block with "<name> as ",
	// and overwrites: the second call wins.
	q := c.Query(ctx).As("first").As("second")
	if q == nil {
		t.Fatal("As() returned nil *RawQuery")
	}
	s := q.String()
	if !strings.Contains(s, "second as ") {
		t.Fatalf("second As not rendered; got:\n%s", s)
	}
	if strings.Contains(s, "first as ") {
		t.Fatalf("first As still present after overwrite; got:\n%s", s)
	}
}

func TestQuery_VarsRendersQueryPrefix(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	// Vars renders a "query <funcDef>" prefix on the generated DQL.
	q := c.Query(ctx).Vars("getByName($n: string)", map[string]string{"$n": "b"})
	s := q.Raw().String()
	if !strings.Contains(s, "query getByName($n: string)") {
		t.Fatalf("Vars did not render the query-definition prefix; got:\n%s", s)
	}
}

func TestQuery_VarsParameterizedQuery(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	for _, n := range []string{"a", "b", "c"} {
		if err := c.Add(ctx, &widget{Name: n}); err != nil {
			t.Fatalf("Add %s: %v", n, err)
		}
	}
	// Vars supplies a GraphQL variable bound into the root function; the
	// query executes via dgraph's QueryWithVars path.
	got, err := c.Query(ctx).
		Vars("getByName($n: string)", map[string]string{"$n": "b"}).
		RootFunc("eq(name, $n)").
		Nodes()
	if err != nil {
		t.Fatalf("Vars query Nodes: %v", err)
	}
	if len(got) != 1 || got[0].Name != "b" {
		t.Fatalf(`Vars parameterized query returned %+v, want one widget named "b"`, got)
	}
}

func TestQuery_VarReturnsRawQuery(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	// Var transitions to *RawQuery and emits a var block: dgman renders the
	// block name as "var".
	rq := c.Query(ctx).Var()
	if rq == nil {
		t.Fatal("Var() returned nil *RawQuery")
	}
	s := rq.String()
	if !strings.Contains(s, "var(func:") {
		t.Fatalf("Var() did not render a var block; got:\n%s", s)
	}
}

func TestQuery_GroupByReturnsRawQuery(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	// GroupBy transitions to *RawQuery and emits an @groupby clause.
	rq := c.Query(ctx).GroupBy("name")
	if rq == nil {
		t.Fatal("GroupBy() returned nil *RawQuery")
	}
	s := rq.String()
	if !strings.Contains(s, "@groupby(name)") {
		t.Fatalf(`GroupBy("name") did not render an @groupby clause; got:\n%s`, s)
	}
}

func TestRawQuery_RawExposesUnderlyingQuery(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	rq := c.Query(ctx).Var()
	// Raw returns the underlying *dgman.Query; String mirrors Raw().String().
	var raw *dgman.Query = rq.Raw()
	if raw == nil {
		t.Fatal("RawQuery.Raw() returned nil")
	}
	if rq.String() != raw.String() {
		t.Fatalf("RawQuery.String() and Raw().String() differ:\n%s\n---\n%s",
			rq.String(), raw.String())
	}
}

func TestRawQuery_GroupByThenVarChains(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	// RawQuery re-exposes Var and GroupBy so the canonical .GroupBy(...).Var()
	// composition still chains; both clauses survive.
	s := c.Query(ctx).GroupBy("name").Var().String()
	if !strings.Contains(s, "@groupby(name)") {
		t.Fatalf("@groupby clause missing after GroupBy().Var(); got:\n%s", s)
	}
	if !strings.Contains(s, "var(func:") {
		t.Fatalf("var block missing after GroupBy().Var(); got:\n%s", s)
	}
}

func TestRawQuery_CarriesEarlierBuilders(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	// Builders applied on *Query[T] before the GroupBy transition survive
	// into the *RawQuery — the two share one underlying *dgman.Query.
	s := c.Query(ctx).Filter(`eq(name, "z")`).GroupBy("name").String()
	if !strings.Contains(s, `eq(name, "z")`) {
		t.Fatalf("Filter set before GroupBy did not survive the transition; got:\n%s", s)
	}
	if !strings.Contains(s, "@groupby(name)") {
		t.Fatalf("@groupby clause missing; got:\n%s", s)
	}
}

// seedOwners inserts owner/pet pairs over conn for the WhereEdge tests. Each
// map entry is one owner owning one pet of the given name; the pet is inserted
// first so the owner's edge links an already-persisted node. It returns an
// owner client bound to conn.
func seedOwners(
	ctx context.Context, t *testing.T, conn dgdao.Client, ownerToPet map[string]string,
) *typed.Client[owner] {
	t.Helper()
	pets := typed.NewClient[pet](conn)
	owners := typed.NewClient[owner](conn)
	for ownerName, petName := range ownerToPet {
		p := &pet{Name: petName}
		if err := pets.Add(ctx, p); err != nil {
			t.Fatalf("Add pet %q: %v", petName, err)
		}
		if err := owners.Add(ctx, &owner{Name: ownerName, Pets: []*pet{p}}); err != nil {
			t.Fatalf("Add owner %q: %v", ownerName, err)
		}
	}
	return owners
}

func TestQuery_WhereEdgeFiltersByEdgeTarget(t *testing.T) {
	ctx := context.Background()
	owners := seedOwners(ctx, t, newConn(t), map[string]string{
		"Alice": "Fido",
		"Bob":   "Rex",
		"Carol": "Fido",
	})

	// WhereEdge constrains owners by a scalar of the pet reached over the
	// "pets" edge — something a root Filter cannot express.
	got, err := owners.Query(ctx).WhereEdge("pets", `eq(name, "Fido")`).Nodes()
	if err != nil {
		t.Fatalf("WhereEdge Nodes: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("WhereEdge(pets, name=Fido) returned %d owners, want 2 (Alice, Carol)", len(got))
	}
	for _, o := range got {
		if o.Name != "Alice" && o.Name != "Carol" {
			t.Fatalf("WhereEdge returned %q, want only Fido owners (Alice, Carol)", o.Name)
		}
	}
}

func TestQuery_WhereEdgeNoMatchReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	owners := seedOwners(ctx, t, newConn(t), map[string]string{"Alice": "Fido", "Bob": "Rex"})

	// No pet is named Nemo: the pre-pass matches zero roots, so Nodes returns
	// an empty result — not an error — and never runs the main query.
	got, err := owners.Query(ctx).WhereEdge("pets", `eq(name, "Nemo")`).Nodes()
	if err != nil {
		t.Fatalf("WhereEdge Nodes: unexpected error %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("WhereEdge for an unowned pet name returned %d owners, want 0", len(got))
	}
}

func TestQuery_WhereEdgeBindsParams(t *testing.T) {
	ctx := context.Background()
	owners := seedOwners(ctx, t, newConn(t), map[string]string{"Alice": "Fido", "Bob": "Rex"})

	// The $1 placeholder in a WhereEdge filter binds exactly as it does for Filter.
	got, err := owners.Query(ctx).WhereEdge("pets", "eq(name, $1)", "Rex").Nodes()
	if err != nil {
		t.Fatalf("WhereEdge Nodes: %v", err)
	}
	if len(got) != 1 || got[0].Name != "Bob" {
		t.Fatalf("WhereEdge(pets, name=$1, Rex) returned %+v, want [Bob]", got)
	}
}

func TestQuery_WhereEdgeCombinesWithFilter(t *testing.T) {
	ctx := context.Background()
	// Alice and Carol both own a Fido; a root Filter on name narrows to Alice.
	owners := seedOwners(ctx, t, newConn(t), map[string]string{
		"Alice": "Fido",
		"Bob":   "Rex",
		"Carol": "Fido",
	})

	got, err := owners.Query(ctx).
		Filter(`eq(name, "Alice")`).
		WhereEdge("pets", `eq(name, "Fido")`).
		Nodes()
	if err != nil {
		t.Fatalf("Filter+WhereEdge Nodes: %v", err)
	}
	if len(got) != 1 || got[0].Name != "Alice" {
		t.Fatalf("Filter(name=Alice)+WhereEdge(pets,name=Fido) returned %+v, want [Alice]", got)
	}
}

func TestQuery_WhereEdgePreservesUIDRoot(t *testing.T) {
	ctx := context.Background()
	conn := newConn(t)
	pets := typed.NewClient[pet](conn)
	owners := typed.NewClient[owner](conn)

	fido := &pet{Name: "Fido"}
	if err := pets.Add(ctx, fido); err != nil {
		t.Fatalf("Add pet: %v", err)
	}
	alice := &owner{Name: "Alice", Pets: []*pet{fido}}
	carol := &owner{Name: "Carol", Pets: []*pet{fido}}
	for _, o := range []*owner{alice, carol} {
		if err := owners.Add(ctx, o); err != nil {
			t.Fatalf("Add owner %q: %v", o.Name, err)
		}
	}

	// Both Alice and Carol own Fido, so the WhereEdge var block matches both.
	// Rooting the query at Alice's UID must survive that match: the var block
	// roots at the caller's UID, so mgMatched is the intersection (just Alice),
	// not every Fido owner.
	got, err := owners.Query(ctx).
		UID(alice.UID).
		WhereEdge("pets", `eq(name, "Fido")`).
		Nodes()
	if err != nil {
		t.Fatalf("UID+WhereEdge Nodes: %v", err)
	}
	if len(got) != 1 || got[0].Name != "Alice" {
		t.Fatalf("UID(Alice)+WhereEdge(pets,name=Fido) returned %+v, want [Alice]", got)
	}
}

func TestQuery_WhereEdgeMultipleConstraintsAnd(t *testing.T) {
	ctx := context.Background()
	conn := newConn(t)
	pets := typed.NewClient[pet](conn)
	owners := typed.NewClient[owner](conn)

	// Alice owns both Fido and Rex; Bob owns only Fido.
	fido, rex := &pet{Name: "Fido"}, &pet{Name: "Rex"}
	for _, p := range []*pet{fido, rex} {
		if err := pets.Add(ctx, p); err != nil {
			t.Fatalf("Add pet %q: %v", p.Name, err)
		}
	}
	if err := owners.Add(ctx, &owner{Name: "Alice", Pets: []*pet{fido, rex}}); err != nil {
		t.Fatalf("Add Alice: %v", err)
	}
	if err := owners.Add(ctx, &owner{Name: "Bob", Pets: []*pet{fido}}); err != nil {
		t.Fatalf("Add Bob: %v", err)
	}

	// Two WhereEdge calls AND together: only an owner of BOTH pets survives.
	got, err := owners.Query(ctx).
		WhereEdge("pets", `eq(name, "Fido")`).
		WhereEdge("pets", `eq(name, "Rex")`).
		Nodes()
	if err != nil {
		t.Fatalf("two-WhereEdge Nodes: %v", err)
	}
	if len(got) != 1 || got[0].Name != "Alice" {
		t.Fatalf("WhereEdge(Fido) AND WhereEdge(Rex) returned %+v, want [Alice]", got)
	}
}

func TestQuery_WhereEdgeFirst(t *testing.T) {
	ctx := context.Background()
	owners := seedOwners(ctx, t, newConn(t), map[string]string{"Alice": "Fido", "Bob": "Rex"})

	// First runs the pre-pass too: it returns the Rex owner, never a Fido one.
	got, err := owners.Query(ctx).WhereEdge("pets", `eq(name, "Rex")`).First()
	if err != nil {
		t.Fatalf("WhereEdge First: %v", err)
	}
	if got == nil || got.Name != "Bob" {
		t.Fatalf("WhereEdge(pets,name=Rex).First() = %+v, want Bob", got)
	}

	// First with an edge constraint nothing satisfies is (nil, nil).
	none, err := owners.Query(ctx).WhereEdge("pets", `eq(name, "Nemo")`).First()
	if err != nil {
		t.Fatalf("WhereEdge First no-match: unexpected error %v", err)
	}
	if none != nil {
		t.Fatalf("WhereEdge First with no match = %+v, want nil", none)
	}
}

func TestQuery_WhereEdgeNodesAndCount(t *testing.T) {
	ctx := context.Background()
	owners := seedOwners(ctx, t, newConn(t), map[string]string{
		"Alice": "Fido",
		"Bob":   "Rex",
		"Carol": "Fido",
		"Dave":  "Fido",
	})

	// Three owners have a Fido. Limit caps the returned rows to 2, but the count
	// reflects the full matched set: the count block runs count(uid) over
	// uid(mgMatched), independent of the data block's pagination.
	rows, count, err := owners.Query(ctx).
		WhereEdge("pets", `eq(name, "Fido")`).
		Limit(2).
		NodesAndCount()
	if err != nil {
		t.Fatalf("WhereEdge NodesAndCount: %v", err)
	}
	if count != 3 {
		t.Fatalf("WhereEdge NodesAndCount count = %d, want 3 (Alice, Carol, Dave)", count)
	}
	if len(rows) != 2 {
		t.Fatalf("WhereEdge NodesAndCount returned %d rows, want 2 (Limit)", len(rows))
	}
}

func TestQuery_WhereEdgeIterNodes(t *testing.T) {
	ctx := context.Background()
	owners := seedOwners(ctx, t, newConn(t), map[string]string{
		"Alice": "Fido",
		"Bob":   "Rex",
		"Carol": "Fido",
	})

	seen := 0
	for o, err := range owners.Query(ctx).WhereEdge("pets", `eq(name, "Fido")`).IterNodes() {
		if err != nil {
			t.Fatalf("WhereEdge IterNodes yielded error: %v", err)
		}
		if o.Name != "Alice" && o.Name != "Carol" {
			t.Fatalf("WhereEdge IterNodes yielded %q, want a Fido owner", o.Name)
		}
		seen++
	}
	if seen != 2 {
		t.Fatalf("WhereEdge IterNodes streamed %d owners, want 2", seen)
	}
}

func TestQuery_WhereEdgeForwardsVars(t *testing.T) {
	ctx := context.Background()
	owners := seedOwners(ctx, t, newConn(t), map[string]string{
		"Alice": "Fido",
		"Bob":   "Fido",
	})

	// Vars binds a GraphQL variable into the root function, combined with a
	// WhereEdge constraint. The WhereEdge path renders its own multi-block
	// request via QueryRaw, so it must forward the variable; otherwise $n is
	// unbound and the query errors. Both own a Fido; $n narrows to Alice.
	got, err := owners.Query(ctx).
		Vars("byName($n: string)", map[string]string{"$n": "Alice"}).
		RootFunc("eq(name, $n)").
		WhereEdge("pets", `eq(name, "Fido")`).
		Nodes()
	if err != nil {
		t.Fatalf("Vars+WhereEdge Nodes: %v", err)
	}
	if len(got) != 1 || got[0].Name != "Alice" {
		t.Fatalf("Vars($n=Alice)+WhereEdge(pets,Fido) returned %+v, want [Alice]", got)
	}
}

func TestQuery_UIDRootsAtNode(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	w := &widget{Name: "sprocket", Qty: 3}
	if err := c.Add(ctx, w); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, err := c.Query(ctx).UID(w.UID).Nodes()
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if len(got) != 1 || got[0].Name != "sprocket" {
		t.Fatalf("UID query returned %+v, want one widget named sprocket", got)
	}
}

func TestQuery_NodesAndCountReturnsTotal(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	for i := 0; i < 3; i++ {
		if err := c.Add(ctx, &widget{Name: "w", Qty: i}); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	nodes, count, err := c.Query(ctx).NodesAndCount()
	if err != nil {
		t.Fatalf("NodesAndCount: %v", err)
	}
	if count != 3 || len(nodes) != 3 {
		t.Fatalf("got count=%d len=%d, want 3 and 3", count, len(nodes))
	}
}

func TestQuery_AllSetsTraversalDepth(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))
	if err := c.Add(ctx, &widget{Name: "deep", Qty: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// All(1) overrides the default traversal depth for this query; the call
	// must chain and the query must still execute and decode.
	got, err := c.Query(ctx).All(1).Nodes()
	if err != nil {
		t.Fatalf("Nodes with All(1): %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d widgets, want 1", len(got))
	}
}

func TestQuery_StringRendersDQL(t *testing.T) {
	ctx := context.Background()
	c := typed.NewClient[widget](newConn(t))

	dql := c.Query(ctx).Filter("eq(name, $1)", "sprocket").String()
	if !strings.Contains(dql, "widget") {
		t.Fatalf("String() = %q, want it to mention the widget type", dql)
	}
}
