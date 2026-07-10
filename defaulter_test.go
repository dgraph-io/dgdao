/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"testing"

	"github.com/go-playground/validator/v10"

	"github.com/dgraph-io/dgdao"
)

// defaultedEntity carries a validate:"required" field that callers leave
// zero. ApplyDefaults stamps it only when it is still zero, so a write
// succeeds only if dgdao invokes ApplyDefaults before validateStruct: this
// is the ordering the Defaulter interface promises.
type defaultedEntity struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Key   string   `json:"key,omitempty" dgraph:"index=hash upsert unique"`
	Name  string   `json:"name,omitempty" validate:"required"`
}

func (e *defaultedEntity) ApplyDefaults(_ context.Context) error {
	if e.Name == "" {
		e.Name = "default-name"
	}
	return nil
}

func newDefaulterClient(t *testing.T) dgdao.Client {
	t.Helper()
	conn, err := dgdao.NewClient("file://"+t.TempDir(), dgdao.WithAutoSchema(true), dgdao.WithValidator(validator.New()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(conn.Close)
	return conn
}

func TestDefaulterAppliesBeforeValidateOnInsert(t *testing.T) {
	conn := newDefaulterClient(t)
	ctx := context.Background()

	obj := &defaultedEntity{Key: "insert-1"}
	if err := conn.Insert(ctx, obj); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if obj.Name != "default-name" {
		t.Fatalf("want Name defaulted to %q, got %q", "default-name", obj.Name)
	}
}

func TestDefaulterAppliesBeforeValidateOnUpsert(t *testing.T) {
	conn := newDefaulterClient(t)
	ctx := context.Background()

	obj := &defaultedEntity{Key: "upsert-1"}
	if err := conn.Upsert(ctx, obj, "key"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if obj.Name != "default-name" {
		t.Fatalf("want Name defaulted to %q, got %q", "default-name", obj.Name)
	}
}

func TestDefaulterAppliesBeforeValidateOnLoadOrStore(t *testing.T) {
	conn := newDefaulterClient(t)
	ctx := context.Background()

	obj := &defaultedEntity{Key: "loadorstore-1"}
	loaded, err := conn.LoadOrStore(ctx, obj, "key")
	if err != nil {
		t.Fatalf("LoadOrStore: %v", err)
	}
	if loaded {
		t.Fatal("want loaded=false (newly created)")
	}
	if obj.Name != "default-name" {
		t.Fatalf("want Name defaulted to %q, got %q", "default-name", obj.Name)
	}
}

func TestDefaulterAppliesBeforeValidateOnUpdate(t *testing.T) {
	conn := newDefaulterClient(t)
	ctx := context.Background()

	obj := &defaultedEntity{Key: "update-1"}
	if err := conn.Insert(ctx, obj); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Simulate a caller that clears the required field before updating;
	// ApplyDefaults must re-stamp it before validateStruct runs, just as it
	// did on the initial Insert.
	obj.Name = ""
	if err := conn.Update(ctx, obj); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if obj.Name != "default-name" {
		t.Fatalf("want Name defaulted to %q, got %q", "default-name", obj.Name)
	}
}

// plainEntity implements no Defaulter; Insert must remain a no-op for it
// rather than panicking or erroring when the write-path dispatch finds no
// Defaulter implementation.
type plainDefaulterEntity struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Name  string   `json:"name,omitempty"`
}

func TestDefaulterNoopWhenNotImplemented(t *testing.T) {
	conn := newDefaulterClient(t)
	ctx := context.Background()

	obj := &plainDefaulterEntity{Name: "unchanged"}
	if err := conn.Insert(ctx, obj); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if obj.Name != "unchanged" {
		t.Fatalf("want Name unchanged, got %q", obj.Name)
	}
}
