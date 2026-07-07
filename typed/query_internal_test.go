/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package typed

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/dgraph-io/dgdao"
	dg "github.com/dolan-in/dgman/v2"
)

// TestRemapPredicateKeys_SurfacesMalformedBlock guards the swallowed-error fix:
// MultiQuery.Execute used to discard a remap error and fall through to a generic
// downstream decode failure. remapPredicateKeys must now return the structural
// error so Execute can wrap it as "remapping block %q".
func TestRemapPredicateKeys_SurfacesMalformedBlock(t *testing.T) {
	// A block body that opens as an array but is not valid JSON cannot be
	// remapped; the error must propagate rather than be swallowed.
	if _, err := remapPredicateKeys([]byte("[not valid json"), reflect.TypeFor[ivOwner]()); err == nil {
		t.Fatal("remapPredicateKeys swallowed a malformed-array error; it must surface it")
	}
	// A well-formed array remaps without error.
	if _, err := remapPredicateKeys([]byte(`[{"name":"x"}]`), reflect.TypeFor[ivOwner]()); err != nil {
		t.Fatalf("remapPredicateKeys errored on valid input: %v", err)
	}
}

type ivPet struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Name  string   `json:"name,omitempty" dgraph:"index=exact"`
}

type ivOwner struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Name  string   `json:"name,omitempty" dgraph:"index=exact"`
	Pets  []*ivPet `json:"pets,omitempty"`
}

// TestEdgeBlocksRenderServerSideVar asserts a WhereEdge query renders as a
// server-side var block consumed via uid(mgMatched). The matched roots are
// bound on the server and never inlined into a uid(<literal>, ...) list, which
// is what keeps WhereEdge bounded regardless of how many roots match — the
// concern that motivated replacing the eager client-side pre-pass.
func TestEdgeBlocksRenderServerSideVar(t *testing.T) {
	conn, err := dgdao.NewClient("file://"+t.TempDir(), dgdao.WithAutoSchema(true))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(conn.Close)

	qb := NewClient[ivOwner](conn).Query(context.Background()).
		Filter(`eq(name, "Alice")`).
		WhereEdge("pets", `eq(name, "Fido")`)

	dql := dg.NewQueryBlock(qb.edgeBlocks(true)...).String()

	for _, want := range []string{
		"mgMatched as var(", // matched roots bound server-side
		"@cascade",          // edge constraint enforced by cascade
		"uid(mgMatched)",    // data and count blocks consume the var
		"count(uid)",        // count block present
	} {
		if !strings.Contains(dql, want) {
			t.Errorf("rendered DQL missing %q:\n%s", want, dql)
		}
	}
	// The owner UIDs must never be inlined into the query — that was the
	// unbounded behavior. A server-side var carries no uid(0x...) literal list.
	if strings.Contains(dql, "uid(0x") {
		t.Errorf("rendered DQL inlines UID literals (unbounded):\n%s", dql)
	}
}
