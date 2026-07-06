/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"fmt"

	mg "github.com/dgraph-io/dgdao"
)

// Actor is a schema-defining record. Implementing mg.Schema (a single
// SchemaTypeName method) marks it as a generated schema type; code generators
// such as dgdao-gen emit this method.
type Actor struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Name  string   `json:"name,omitempty" dgraph:"index=exact"`
}

func (a *Actor) SchemaTypeName() string { return "Actor" }

// ActorBuilder is a wrapper around Actor — the shape a generated fluent builder
// or domain wrapper takes. Exposing Unwrap lets the dgdao client route the
// wrapper to its backing record, so the wrapper can be passed straight to
// Insert/Update/Get without the caller reaching for the inner value.
type ActorBuilder struct{ actor *Actor }

func (b *ActorBuilder) Unwrap() *Actor { return b.actor }

// ExampleSchema shows the wrapper pattern: the client unwraps an ActorBuilder
// to its Actor before persisting, so generated wrapper types work transparently
// while plain structs are unaffected.
func ExampleSchema() {
	client, _ := mg.NewClient("dgraph://localhost:9080")
	defer client.Close()

	ctx := context.Background()
	builder := &ActorBuilder{actor: &Actor{Name: "Sigourney Weaver"}}

	// Insert the wrapper; the client unwraps it to the Actor record.
	if err := client.Insert(ctx, builder); err != nil {
		panic(err)
	}
	fmt.Println(builder.actor.Name)
}
