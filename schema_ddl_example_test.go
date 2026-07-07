/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"

	mg "github.com/dgraph-io/dgdao"
)

// ExampleClient_AlterSchema applies a raw DQL schema string directly, giving
// full control over predicate types, indexes, and directives. This complements
// UpdateSchema (which infers the schema from Go struct tags) and is useful for
// migrations that declare predicates no Go type models yet.
func ExampleClient_AlterSchema() {
	client, _ := mg.NewClient("dgraph://localhost:9080")
	defer client.Close()

	ctx := context.Background()
	schema := `
		name: string @index(exact) .
		email: string @index(hash) @upsert .
		age: int @index(int) .
	`
	if err := client.AlterSchema(ctx, schema); err != nil {
		panic(err)
	}
}
