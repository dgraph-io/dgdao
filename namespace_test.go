/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"testing"

	"github.com/dgraph-io/dgdao"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/stretchr/testify/require"
)

func TestNonGalaxyDB(t *testing.T) {
	engine, err := dgdao.NewEngine(dgdao.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(context.Background()))
	require.NoError(t, ns1.AlterSchema(context.Background(), "name: string @index(exact) ."))

	_, err = ns1.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Subject:     "_:aman",
					Predicate:   "name",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "A"}},
				},
			},
		},
	})
	require.NoError(t, err)

	query := `{
			me(func: has(name)) {
				name
			}
		}`
	resp, err := ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"A"}]}`, string(resp.GetJson()))

}

func TestDropDataNamespace(t *testing.T) {
	engine, err := dgdao.NewEngine(dgdao.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, ns1.DropData(context.Background()))
	require.NoError(t, ns1.AlterSchema(context.Background(), "name: string @index(exact) ."))

	_, err = ns1.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Subject:     "_:aman",
					Predicate:   "name",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "A"}},
				},
			},
		},
	})
	require.NoError(t, err)

	query := `{
			me(func: has(name)) {
				name
			}
		}`
	resp, err := ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"A"}]}`, string(resp.GetJson()))

	require.NoError(t, ns1.DropData(context.Background()))

	resp, err = ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[]}`, string(resp.GetJson()))
}

func TestMultipleDBs(t *testing.T) {
	engine, err := dgdao.NewEngine(dgdao.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	db0, err := engine.GetNamespace(0)
	require.NoError(t, err)
	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, engine.DropAll(context.Background()))
	require.NoError(t, db0.AlterSchema(context.Background(), "name: string @index(exact) ."))
	require.NoError(t, ns1.AlterSchema(context.Background(), "name: string @index(exact) ."))

	_, err = db0.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Subject:     "_:aman",
					Predicate:   "name",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "A"}},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = ns1.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Subject:     "_:aman",
					Predicate:   "name",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "B"}},
				},
			},
		},
	})
	require.NoError(t, err)

	query := `{
			me(func: has(name)) {
				name
			}
		}`
	resp, err := db0.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"A"}]}`, string(resp.GetJson()))

	resp, err = ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"B"}]}`, string(resp.GetJson()))

	require.NoError(t, ns1.DropData(context.Background()))
	resp, err = ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[]}`, string(resp.GetJson()))
}

func TestQueryWrongDB(t *testing.T) {
	engine, err := dgdao.NewEngine(dgdao.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	db0, err := engine.GetNamespace(0)
	require.NoError(t, err)
	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, engine.DropAll(context.Background()))
	require.NoError(t, db0.AlterSchema(context.Background(), "name: string @index(exact) ."))
	require.NoError(t, ns1.AlterSchema(context.Background(), "name: string @index(exact) ."))

	_, err = db0.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Namespace:   1,
					Subject:     "_:aman",
					Predicate:   "name",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "A"}},
				},
			},
		},
	})
	require.NoError(t, err)

	query := `{
		me(func: has(name)) {
			name
		}
	}`

	resp, err := ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[]}`, string(resp.GetJson()))
}

func TestTwoDBs(t *testing.T) {
	engine, err := dgdao.NewEngine(dgdao.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	db0, err := engine.GetNamespace(0)
	require.NoError(t, err)
	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, engine.DropAll(context.Background()))
	require.NoError(t, db0.AlterSchema(context.Background(), "foo: string @index(exact) ."))
	require.NoError(t, ns1.AlterSchema(context.Background(), "bar: string @index(exact) ."))

	_, err = db0.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Subject:     "_:aman",
					Predicate:   "foo",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "A"}},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = ns1.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Subject:     "_:aman",
					Predicate:   "bar",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "B"}},
				},
			},
		},
	})
	require.NoError(t, err)

	query := `{
		me(func: has(foo)) {
			foo
		}
	}`
	resp, err := db0.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"foo":"A"}]}`, string(resp.GetJson()))

	query = `{
		me(func: has(bar)) {
			bar
		}
	}`
	resp, err = ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"bar":"B"}]}`, string(resp.GetJson()))
}

func TestCommitOrAbortNamespaceIsolation(t *testing.T) {
	engine, err := dgdao.NewEngine(dgdao.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	// Create two separate namespaces
	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)

	ns2, err := engine.CreateNamespace()
	require.NoError(t, err)

	// Set up schema in both namespaces
	require.NoError(t, ns1.AlterSchema(context.Background(), "name: string @index(exact) ."))
	require.NoError(t, ns2.AlterSchema(context.Background(), "name: string @index(exact) ."))

	// Add data to namespace 1
	_, err = ns1.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Subject:     "_:entity1",
					Predicate:   "name",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "Namespace1Entity"}},
				},
			},
		},
	})
	require.NoError(t, err)

	// Add data to namespace 2
	_, err = ns2.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Subject:     "_:entity2",
					Predicate:   "name",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "Namespace2Entity"}},
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify data is isolated - each namespace should only see its own data
	query := `{
		me(func: has(name)) {
			name
		}
	}`

	resp1, err := ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"Namespace1Entity"}]}`, string(resp1.GetJson()))

	resp2, err := ns2.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"Namespace2Entity"}]}`, string(resp2.GetJson()))

	// Verify that dropping data from one namespace doesn't affect the other
	require.NoError(t, ns1.DropData(context.Background()))

	resp1After, err := ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[]}`, string(resp1After.GetJson()))

	resp2After, err := ns2.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"name":"Namespace2Entity"}]}`, string(resp2After.GetJson()))
}

func TestDBDBRestart(t *testing.T) {
	dataDir := t.TempDir()
	engine, err := dgdao.NewEngine(dgdao.NewDefaultConfig(dataDir))
	require.NoError(t, err)
	defer func() { engine.Close() }()

	ns1, err := engine.CreateNamespace()
	require.NoError(t, err)
	ns1Id := ns1.ID()

	require.NoError(t, ns1.AlterSchema(context.Background(), "bar: string @index(exact) ."))
	_, err = ns1.Mutate(context.Background(), []*api.Mutation{
		{
			Set: []*api.NQuad{
				{
					Subject:     "_:aman",
					Predicate:   "bar",
					ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: "B"}},
				},
			},
		},
	})
	require.NoError(t, err)

	engine.Close()
	engine, err = dgdao.NewEngine(dgdao.NewDefaultConfig(dataDir))
	require.NoError(t, err)

	db2, err := engine.CreateNamespace()
	require.NoError(t, err)
	require.Greater(t, db2.ID(), ns1Id)

	ns1, err = engine.GetNamespace(ns1Id)
	require.NoError(t, err)

	query := `{
		me(func: has(bar)) {
			bar
		}
	}`
	resp, err := ns1.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"bar":"B"}]}`, string(resp.GetJson()))
}
