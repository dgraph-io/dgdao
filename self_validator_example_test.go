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

// Event carries a cross-field rule that struct tags cannot express: End must
// not precede Start. Implementing SelfValidator lets the type enforce that rule
// itself; the client calls ValidateWith on Insert/Upsert/Update. ValidateWith
// also receives the configured StructValidator, so a type can run ordinary
// tag-based validation first and then layer custom logic on top.
type Event struct {
	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Name  string   `json:"name,omitempty"`
	Start int      `json:"start,omitempty"`
	End   int      `json:"end,omitempty"`
}

func (e *Event) ValidateWith(ctx context.Context, v mg.StructValidator) error {
	// Run any tag-based validation the client was configured with.
	if v != nil {
		if err := v.StructCtx(ctx, e); err != nil {
			return err
		}
	}
	// Then the cross-field rule.
	if e.End < e.Start {
		return fmt.Errorf("event %q: End (%d) must be >= Start (%d)", e.Name, e.End, e.Start)
	}
	return nil
}

// ExampleSelfValidator inserts an Event; the client routes it through
// ValidateWith, so the cross-field rule runs before the write.
func ExampleSelfValidator() {
	client, _ := mg.NewClient("dgraph://localhost:9080")
	defer client.Close()

	ctx := context.Background()
	err := client.Insert(ctx, &Event{Name: "launch", Start: 10, End: 5})
	fmt.Println(err != nil) // the rule rejects End < Start
}
