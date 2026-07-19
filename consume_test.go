/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"testing"

	"github.com/dgraph-io/dgdao"
)

type consumeJTI struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	JTI   string   `json:"jti,omitempty" dgraph:"index=hash upsert unique"`
}

func newConsumeClient(t *testing.T) dgdao.Client {
	t.Helper()
	conn, err := dgdao.NewClient("file://"+t.TempDir(), dgdao.WithAutoSchema(true))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(conn.Close)
	return conn
}

func TestGetOrInsert(t *testing.T) {
	conn := newConsumeClient(t)
	ctx := context.Background()

	first := &consumeJTI{JTI: "abc"}
	loaded, err := conn.GetOrInsert(ctx, first, "jti")
	if err != nil {
		t.Fatalf("first GetOrInsert: %v", err)
	}
	if loaded {
		t.Fatal("first store: want loaded=false (newly created)")
	}

	second := &consumeJTI{JTI: "abc"}
	loaded, err = conn.GetOrInsert(ctx, second, "jti")
	if err != nil {
		t.Fatalf("second GetOrInsert: %v", err)
	}
	if !loaded {
		t.Fatal("second store: want loaded=true (already existed)")
	}
	// On the loaded=true path, GetOrInsert must hydrate the passed object with
	// the existing record. Assert it carries the existing node's UID, so a
	// regression that returns loaded=true without populating fields is caught.
	if second.UID == "" {
		t.Fatal("second store: loaded record was not hydrated (UID empty)")
	}
	if second.UID != first.UID {
		t.Fatalf("second store: want existing node UID %q, got %q", first.UID, second.UID)
	}
}

type consumeState struct {
	UID    string   `json:"uid,omitempty"`
	DType  []string `json:"dgraph.type,omitempty"`
	State  string   `json:"state,omitempty" dgraph:"index=hash upsert"`
	Secret string   `json:"secret,omitempty"`
}

func TestGetAndDelete(t *testing.T) {
	conn := newConsumeClient(t)
	ctx := context.Background()

	if err := conn.Insert(ctx, &consumeState{State: "s1", Secret: "shh"}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	var got consumeState
	loaded, err := conn.GetAndDelete(ctx, &got, "s1", "state")
	if err != nil {
		t.Fatalf("GetAndDelete: %v", err)
	}
	if !loaded {
		t.Fatal("first consume: want loaded=true")
	}
	if got.Secret != "shh" {
		t.Fatalf("want prior secret %q, got %q", "shh", got.Secret)
	}

	var again consumeState
	loaded, err = conn.GetAndDelete(ctx, &again, "s1", "state")
	if err != nil {
		t.Fatalf("second GetAndDelete: %v", err)
	}
	if loaded {
		t.Fatal("second consume: want loaded=false (already consumed)")
	}
}

// GetAndDelete concatenates the predicate name straight into the DQL filter, so
// a name carrying DQL metacharacters must be rejected before it reaches the query
// rather than corrupting or injecting it. The key value is separately
// parameterized, so only the predicate name is at risk here.
func TestGetAndDeleteRejectsInvalidPredicate(t *testing.T) {
	conn := newConsumeClient(t)
	ctx := context.Background()

	for _, pred := range []string{
		"state), has(secret", // break out of eq(...) and inject another function
		"state, uid",         // extra argument
		"state\"",            // stray quote
		"has(secret)",        // whole different function
		"state val",          // embedded whitespace
	} {
		var got consumeState
		loaded, err := conn.GetAndDelete(ctx, &got, "s1", pred)
		if err == nil {
			t.Fatalf("predicate %q: want error, got nil (loaded=%v)", pred, loaded)
		}
		if loaded {
			t.Fatalf("predicate %q: want loaded=false on rejection, got true", pred)
		}
	}

	// A valid predicate still works: the guard must not reject legitimate names.
	if err := conn.Insert(ctx, &consumeState{State: "ok", Secret: "shh"}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	var got consumeState
	loaded, err := conn.GetAndDelete(ctx, &got, "ok", "state")
	if err != nil {
		t.Fatalf("valid predicate: unexpected error: %v", err)
	}
	if !loaded {
		t.Fatal("valid predicate: want loaded=true")
	}
}

// A new public API must return errors on nil input rather than panic. A nil
// interface would panic in reflect.TypeOf(nil).Kind(); a typed nil pointer would
// pass the pointer-kind check and then panic when uidOf dereferences it.
func TestGetAndDeleteRejectsNilObject(t *testing.T) {
	conn := newConsumeClient(t)
	ctx := context.Background()

	if _, err := conn.GetAndDelete(ctx, nil, "s1", "state"); err == nil {
		t.Fatal("nil interface: want error, got nil")
	}
	if _, err := conn.GetAndDelete(ctx, (*consumeState)(nil), "s1", "state"); err == nil {
		t.Fatal("typed nil pointer: want error, got nil")
	}
}

// GetOrInsert validates via checkPointer (validateStruct is a no-op without a
// configured validator), so nil and typed-nil input must return an error rather
// than reaching MutateOrGet and panicking.
func TestGetOrInsertRejectsNilObject(t *testing.T) {
	conn := newConsumeClient(t)
	ctx := context.Background()

	if _, err := conn.GetOrInsert(ctx, nil, "jti"); err == nil {
		t.Fatal("nil interface: want error, got nil")
	}
	if _, err := conn.GetOrInsert(ctx, (*consumeJTI)(nil), "jti"); err == nil {
		t.Fatal("typed nil pointer: want error, got nil")
	}
}

// GetAndDelete documents that obj is left zero when loaded=false. A caller may
// pass a pre-populated struct (and, after a commit-abort retry, a prior Get may
// have hydrated it), so a not-found consume must reset obj rather than leave
// stale data behind.
func TestGetAndDeleteZeroesObjectWhenNotFound(t *testing.T) {
	conn := newConsumeClient(t)
	ctx := context.Background()

	got := consumeState{UID: "0x99", State: "preexisting", Secret: "stale"}
	loaded, err := conn.GetAndDelete(ctx, &got, "no-such-key", "state")
	if err != nil {
		t.Fatalf("GetAndDelete: %v", err)
	}
	if loaded {
		t.Fatal("want loaded=false for a missing key")
	}
	if got.UID != "" || got.State != "" || got.Secret != "" || got.DType != nil {
		t.Fatalf("want obj zeroed on loaded=false, got %+v", got)
	}
}

// consumeLowerUID stores its uid in a field named Uid, so dgman hydrates it
// (json:"uid") but uidOf's FieldByName("UID") cannot read it.
type consumeLowerUID struct {
	Uid   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	State string   `json:"state,omitempty" dgraph:"index=hash upsert"`
}

// When Get matches a node but its UID cannot be read (the model lacks a UID
// string field), GetAndDelete must error rather than report loaded=false: the
// node existed and would otherwise be silently skipped instead of consumed.
func TestGetAndDeleteErrorsWhenUIDUnreadable(t *testing.T) {
	conn := newConsumeClient(t)
	ctx := context.Background()

	if err := conn.Insert(ctx, &consumeLowerUID{State: "s1"}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	var got consumeLowerUID
	loaded, err := conn.GetAndDelete(ctx, &got, "s1", "state")
	if err == nil {
		t.Fatalf("want error when the matched node's UID is unreadable, got nil (loaded=%v)", loaded)
	}
	if loaded {
		t.Fatal("want loaded=false alongside the error")
	}
}
