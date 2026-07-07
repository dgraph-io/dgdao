/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"fmt"

	dg "github.com/dgraph-io/dgdao"
)

// Token is keyed by a unique jti predicate. The upsert+unique tags let
// LoadOrStore atomically insert-if-absent on that key.
type Token struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	JTI   string   `json:"jti,omitempty" dgraph:"index=hash upsert unique"`
}

// ExampleClient_loadOrStore atomically inserts a node if no node with the same
// key exists, or reports that one already did. loaded is false when this call
// created the node and true when an existing node was found; on the loaded=true
// path the passed object is hydrated with the existing record.
//
// This is the building block for "claim a one-time token": the first caller
// stores and proceeds, every later caller sees loaded=true and is rejected.
func ExampleClient_LoadOrStore() {
	client, _ := dg.NewClient("dgraph://localhost:9080")
	defer client.Close()

	ctx := context.Background()
	loaded, err := client.LoadOrStore(ctx, &Token{JTI: "abc123"}, "jti")
	if err != nil {
		panic(err)
	}
	fmt.Println(loaded) // false the first time, true thereafter
}

// ExampleClient_loadAndDelete atomically reads a node and deletes it, electing a
// single winner under concurrency: exactly one caller gets loaded=true with the
// record hydrated, the rest get loaded=false. Use it to consume a one-shot
// value — a nonce, a pending job, a single-use code.
func ExampleClient_LoadAndDelete() {
	client, _ := dg.NewClient("dgraph://localhost:9080")
	defer client.Close()

	ctx := context.Background()
	var got Token
	loaded, err := client.LoadAndDelete(ctx, &got, "abc123", "jti")
	if err != nil {
		panic(err)
	}
	if loaded {
		fmt.Println("consumed", got.JTI)
	}
}
