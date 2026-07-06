/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package filter_test

import (
	"fmt"

	"github.com/dgraph-io/dgdao/typed/filter"
)

// ExampleBuilder composes a parameterised @filter expression. Terms within a
// group join with OR; groups join with AND; required terms form their own
// group. Build returns the expression and the positional params that
// typed.Query[T].Filter consumes — the values never get interpolated into the
// string, so the expression is safe against injection.
func ExampleBuilder() {
	var b filter.Builder
	b.RequiredEq("archiveStatus", "Active")
	b.EqGroupString("name", []filter.String{{Value: "Alice"}, {Value: "Bob"}})

	expr, params := b.Build()
	fmt.Println(expr)
	fmt.Println(params...)
	// Output:
	// eq(archiveStatus, $1) AND (eq(name, $2) OR eq(name, $3))
	// Active Alice Bob
}
