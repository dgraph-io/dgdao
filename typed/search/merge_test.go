/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package search_test

import (
	"reflect"
	"testing"

	"github.com/dgraph-io/dgdao/typed/search"
)

type rec struct {
	ID  string
	Tag string
}

func id(r rec) string { return r.ID }

func TestMergeByID(t *testing.T) {
	cases := []struct {
		name   string
		inputs [][]rec
		want   []rec
	}{
		{
			name:   "empty inputs returns nil",
			inputs: nil,
			want:   nil,
		},
		{
			name:   "single empty slice returns nil",
			inputs: [][]rec{{}},
			want:   nil,
		},
		{
			name: "single slice returns it as-is",
			inputs: [][]rec{{
				{ID: "a", Tag: "1"},
				{ID: "b", Tag: "1"},
			}},
			want: []rec{
				{ID: "a", Tag: "1"},
				{ID: "b", Tag: "1"},
			},
		},
		{
			name: "two slices merge in priority order",
			inputs: [][]rec{
				{{ID: "a", Tag: "name"}},
				{{ID: "b", Tag: "desc"}},
			},
			want: []rec{
				{ID: "a", Tag: "name"},
				{ID: "b", Tag: "desc"},
			},
		},
		{
			name: "duplicate ID keeps first-seen entry",
			inputs: [][]rec{
				{{ID: "a", Tag: "name"}},
				{{ID: "a", Tag: "desc"}, {ID: "b", Tag: "desc"}},
			},
			want: []rec{
				{ID: "a", Tag: "name"},
				{ID: "b", Tag: "desc"},
			},
		},
		{
			name: "intra-slice duplicates dedup too",
			inputs: [][]rec{
				{{ID: "a", Tag: "1"}, {ID: "a", Tag: "2"}, {ID: "b", Tag: "1"}},
			},
			want: []rec{
				{ID: "a", Tag: "1"},
				{ID: "b", Tag: "1"},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := search.MergeByID(id, c.inputs...)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}
