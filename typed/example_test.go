/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package typed_test

import (
	"context"
	"fmt"

	"github.com/dgraph-io/dgdao"
	"github.com/dgraph-io/dgdao/typed"
)

// Person is the schema struct the examples bind a typed client to. dgdao
// reflects over the dgraph/json tags, so the type needs no special interface.
type Person struct {
	UID     string    `json:"uid,omitempty"`
	DType   []string  `json:"dgraph.type,omitempty"`
	Name    string    `json:"name,omitempty" dgraph:"index=exact"`
	Age     int       `json:"age,omitempty" dgraph:"index=int"`
	Friends []*Person `json:"friends,omitempty"`
}

// ExampleClient shows the core lift package typed provides: declare the type
// once at construction, then Add, Get, and Query in terms of *Person rather
// than any.
func ExampleClient() {
	conn, _ := dgdao.NewClient("dgraph://localhost:9080")
	defer conn.Close()

	people := typed.NewClient[Person](conn)
	ctx := context.Background()

	alice := &Person{Name: "Alice", Age: 30}
	if err := people.Insert(ctx, alice); err != nil { // Add writes the new UID back into alice.
		panic(err)
	}

	got, err := people.Get(ctx, alice.UID) // got is *Person, not any.
	if err != nil {
		panic(err)
	}
	fmt.Println(got.Name)
}

// ExampleClient_query builds a filtered, ordered, paged query. The terminal
// returns []Person directly — no destination slice to declare, no decode step.
func ExampleClient_query() {
	conn, _ := dgdao.NewClient("dgraph://localhost:9080")
	defer conn.Close()
	people := typed.NewClient[Person](conn)
	ctx := context.Background()

	adults, err := people.Query(ctx).
		Filter("ge(age, $1)", 18).
		OrderAsc("name").
		Limit(50).
		Nodes()
	if err != nil {
		panic(err)
	}
	fmt.Println(len(adults))
}

// ExampleQuery_First returns a single record or nil, replacing the
// declare-slice-then-index-element-zero idiom of the untyped client.
func ExampleQuery_First() {
	conn, _ := dgdao.NewClient("dgraph://localhost:9080")
	defer conn.Close()
	people := typed.NewClient[Person](conn)
	ctx := context.Background()

	person, err := people.Query(ctx).
		Filter("eq(name, $1)", "Alice").
		First()
	if err != nil {
		panic(err)
	}
	if person == nil {
		fmt.Println("not found")
		return
	}
	fmt.Println(person.Name)
}

// ExampleQuery_OrGroup ANDs a scalar filter with an OR of two sub-scopes:
// age >= 18 AND (name == "Alice" OR name == "Bob"). Each sub-scope is a
// detached Query whose filter is captured, not executed.
func ExampleQuery_OrGroup() {
	conn, _ := dgdao.NewClient("dgraph://localhost:9080")
	defer conn.Close()
	people := typed.NewClient[Person](conn)
	ctx := context.Background()

	got, err := people.Query(ctx).
		Filter("ge(age, $1)", 18).
		OrGroup(
			typed.NewDetachedQuery[Person]().Filter(`eq(name, "Alice")`),
			typed.NewDetachedQuery[Person]().Filter(`eq(name, "Bob")`),
		).
		Nodes()
	if err != nil {
		panic(err)
	}
	fmt.Println(len(got))
}

// ExampleQuery_WhereEdge constrains people by a scalar of a neighbour reached
// over the "friends" edge — something a root filter cannot express. The builder
// resolves it with a pre-pass and intersects with any root you set.
func ExampleQuery_WhereEdge() {
	conn, _ := dgdao.NewClient("dgraph://localhost:9080")
	defer conn.Close()
	people := typed.NewClient[Person](conn)
	ctx := context.Background()

	// Everyone who has a friend named "Alice".
	got, err := people.Query(ctx).
		WhereEdge("friends", `eq(name, $1)`, "Alice").
		Nodes()
	if err != nil {
		panic(err)
	}
	fmt.Println(len(got))
}

// ExampleQuery_IterNodes streams a large result set one page at a time over a
// single consistent snapshot, so the whole set is never held in memory at once.
func ExampleClient_iter() {
	conn, _ := dgdao.NewClient("dgraph://localhost:9080")
	defer conn.Close()
	people := typed.NewClient[Person](conn)
	ctx := context.Background()

	for person, err := range people.Query(ctx).OrderAsc("name").IterNodes() {
		if err != nil {
			panic(err)
		}
		fmt.Println(person.Name)
	}
}

// ExampleMultiQuery batches several same-type queries into one Dgraph
// round-trip, keyed by block name.
func ExampleMultiQuery() {
	conn, _ := dgdao.NewClient("dgraph://localhost:9080")
	defer conn.Close()
	people := typed.NewClient[Person](conn)
	ctx := context.Background()

	mq := typed.NewMultiQuery[Person](conn).
		Add("adults", people.Query(ctx).Filter("ge(age, $1)", 18)).
		Add("named_alice", people.Query(ctx).Filter(`eq(name, "Alice")`))

	results, err := mq.Execute(ctx) // one round-trip
	if err != nil {
		panic(err)
	}
	fmt.Println(len(results["adults"]), len(results["named_alice"]))
}
