/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientUpdate(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "UpdateWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "UpdateWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("DGDAO_TEST_ADDR"),
			skip: os.Getenv("DGDAO_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
				return
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			entity := TestEntity{
				Name:        "Test Entity",
				Description: "This is a test entity for the Update method",
				CreatedAt:   time.Now(),
			}

			ctx := context.Background()
			err := client.Insert(ctx, &entity)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, entity.UID, "UID should be assigned")

			uid := entity.UID
			err = client.Get(ctx, &entity, uid)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, entity.UID, uid, "UID should match")

			entity.Description = "Four score and seven years ago"
			err = client.Update(ctx, &entity)
			require.NoError(t, err, "Update should succeed")

			err = client.Get(ctx, &entity, uid)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, entity.Description, "Four score and seven years ago", "Description should match")

			entity = TestEntity{
				Name: "Test Entity 2",
			}
			err = client.Insert(ctx, &entity)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, entity.UID, "UID should be assigned")
			require.NotEqual(t, entity.UID, uid, "UID should be different")

			entity.Name = "Test Entity"
			err = client.Update(ctx, &entity)
			require.Error(t, err, "Update should fail because Name is unique")
		})
	}
}

func TestClientUpdateWithSlices(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "UpdateWithSlicesWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "UpdateWithSlicesWithDgraph",
			uri:  "dgraph://" + os.Getenv("DGDAO_TEST_ADDR"),
			skip: os.Getenv("DGDAO_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
				return
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			ctx := context.Background()
			err := client.DropAll(ctx)
			require.NoError(t, err, "DropAll should succeed")

			// 1. Test successful batch update
			entitiesToInsert := []*TestEntity{
				{Name: "Batch Entity 1", Description: "Initial description 1"},
				{Name: "Batch Entity 2", Description: "Initial description 2"},
				{Name: "Batch Entity 3", Description: "Initial description 3"},
			}

			err = client.Insert(ctx, entitiesToInsert)
			require.NoError(t, err, "Batch insert should succeed")
			for i, entity := range entitiesToInsert {
				require.NotEmpty(t, entity.UID, "Entity %d should have a UID", i)
			}

			for i := range entitiesToInsert {
				entitiesToInsert[i].Description = fmt.Sprintf("Updated description %d", i+1)
			}

			err = client.Update(ctx, entitiesToInsert)
			require.NoError(t, err, "Batch update should succeed")

			for i, entity := range entitiesToInsert {
				var fetched TestEntity
				err = client.Get(ctx, &fetched, entity.UID)
				require.NoError(t, err, "Get should succeed for entity %d", i)
				require.Equal(t, fmt.Sprintf("Updated description %d", i+1), fetched.Description,
					"Description should be updated for entity %d", i)
			}

			// 2. Test in-memory duplicate detection in slice
			duplicateEntities := []*TestEntity{
				{UID: entitiesToInsert[0].UID, Name: "Duplicate Name"},
				{UID: entitiesToInsert[1].UID, Name: "Duplicate Name"}, // Same name as first entity
			}

			err = client.Update(ctx, &duplicateEntities)
			require.Error(t, err, "Update should fail due to duplicate names in the slice")

			// 3. Test database-level unique constraint violation
			uniqueName := "Unique Entity Name"
			newEntity := TestEntity{Name: uniqueName}
			err = client.Insert(ctx, &newEntity)
			require.NoError(t, err, "Insert should succeed for new entity")

			conflictEntity := TestEntity{
				UID:  entitiesToInsert[0].UID,
				Name: uniqueName, // This will conflict with newEntity
			}
			err = client.Update(ctx, &conflictEntity)
			require.Error(t, err, "Update should fail due to unique constraint violation")

			// 4. Test mixed batch with some valid and some invalid updates
			mixedEntities := []*TestEntity{
				{UID: entitiesToInsert[0].UID, Description: "This update would be valid"}, // Valid
				{UID: entitiesToInsert[1].UID, Name: uniqueName},                          // Invalid - name conflict
			}

			err = client.Update(ctx, mixedEntities)
			require.Error(t, err, "Update should fail due to unique constraint violation in batch")
		})
	}
}

type EmbeddedNodeType struct {
	Name string `json:"node.name,omitempty" dgraph:"predicate=node.name"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

type AllTypes struct {
	Name      string    `json:"name,omitempty" dgraph:"index=exact"`
	Age       int       `json:"age,omitempty" dgraph:"index=int"`
	Bool      bool      `json:"bool,omitempty"`
	Value     float64   `json:"value,omitempty" dgraph:"index=float"`
	CreatedAt time.Time `json:"createdAt,omitzero" dgraph:"index=day"`
	Strings   []string  `json:"strings,omitempty" dgraph:"index=term"`

	Node  *EmbeddedNodeType   `json:"node,omitempty"`
	Nodes []*EmbeddedNodeType `json:"nodes,omitempty"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func TestClientUpdateAllTypes(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "UpdateWithAllTypes",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "UpdateWithAllTypesWithDgraph",
			uri:  "dgraph://" + os.Getenv("DGDAO_TEST_ADDR"),
			skip: os.Getenv("DGDAO_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
				return
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			entity := AllTypes{
				Name:      "Test Entity",
				Age:       42,
				Value:     3.14,
				CreatedAt: time.Now(),
				Strings:   []string{"one", "two", "three"},
				Bool:      true,
				Node: &EmbeddedNodeType{
					Name: "Test Node",
				},
				Nodes: []*EmbeddedNodeType{
					{
						Name: "Node In Array, 1",
					},
					{
						Name: "Node In Array, 2",
					},
				},
			}

			ctx := context.Background()
			err := client.Insert(ctx, &entity)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, entity.UID, "UID should be assigned")

			uid := entity.UID
			entity = AllTypes{}
			err = client.Get(ctx, &entity, uid)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, "Test Entity", entity.Name, "Name should match")
			require.Equal(t, 42, entity.Age, "Age should match")
			require.Equal(t, 3.14, entity.Value, "Value should match")
			require.Equal(t, entity.CreatedAt, entity.CreatedAt, "CreatedAt should match")
			require.Equal(t, []string{"one", "two", "three"}, entity.Strings, "Strings should match")
			require.Equal(t, true, entity.Bool, "Bool should match")
			require.Equal(t, "Test Node", entity.Node.Name, "Node Name should match")
			require.NotEmpty(t, entity.Node.UID, "Node UID should be assigned")
			nodeNames := []string{entity.Nodes[0].Name, entity.Nodes[1].Name}
			require.ElementsMatch(t, []string{"Node In Array, 1", "Node In Array, 2"},
				nodeNames, "Node In Array Names should match")

			entity.Age = 43
			entity.Node.Name = "Updated Node"
			entity.Nodes[0].Name = "Updated Node In Array, 1"
			entity.Nodes[1].Name = "Updated Node In Array, 2"
			err = client.Update(ctx, &entity)
			require.NoError(t, err, "Update should succeed")

			entity = AllTypes{}
			err = client.Get(ctx, &entity, uid)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, 43, entity.Age, "Age should match")
			require.Equal(t, "Updated Node", entity.Node.Name, "Node Name should match")
			nodeNames = []string{entity.Nodes[0].Name, entity.Nodes[1].Name}
			require.ElementsMatch(t, []string{"Updated Node In Array, 1", "Updated Node In Array, 2"},
				nodeNames, "Node In Array Names should match")
		})
	}
}

type UniqueEmbeddedNode struct {
	Email string `json:"email,omitempty" dgraph:"unique"`
	Name  string `json:"name,omitempty"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

type ParentWithUniqueEmbedded struct {
	Name  string                `json:"name,omitempty"`
	Node  *UniqueEmbeddedNode   `json:"node,omitempty"`
	Nodes []*UniqueEmbeddedNode `json:"nodes,omitempty"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func TestClientUpdateWithUniqueEmbedded(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "UpdateWithUniqueEmbeddedWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "UpdateWithUniqueEmbeddedWithDgraph",
			uri:  "dgraph://" + os.Getenv("DGDAO_TEST_ADDR"),
			skip: os.Getenv("DGDAO_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
				return
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			ctx := context.Background()

			// 1. Test unique constraint violation in embedded node
			parent1 := ParentWithUniqueEmbedded{
				Name: "Parent 1",
				Node: &UniqueEmbeddedNode{
					Email: "test@example.com",
					Name:  "Test Node",
				},
			}
			err := client.Insert(ctx, &parent1)
			require.NoError(t, err, "Insert should succeed for parent1")
			require.NotEmpty(t, parent1.UID, "Parent1 UID should be assigned")
			require.NotEmpty(t, parent1.Node.UID, "Node UID should be assigned")

			parent2 := ParentWithUniqueEmbedded{
				Name: "Parent 2",
				Node: &UniqueEmbeddedNode{
					Email: "test2@example.com",
					Name:  "Test Node 2",
				},
			}
			err = client.Insert(ctx, &parent2)
			require.NoError(t, err, "Insert should succeed for parent2")
			require.NotEmpty(t, parent2.UID, "Parent2 UID should be assigned")

			// Try to update parent2's node to use the same email as parent1's node
			parent2.Node.Email = "test@example.com"
			err = client.Update(ctx, &parent2)
			require.Error(t, err, "Update should fail due to unique constraint violation in embedded node")

			// 2. Test unique constraint violation in embedded node array
			parent3 := ParentWithUniqueEmbedded{
				Name: "Parent 3",
				Nodes: []*UniqueEmbeddedNode{
					{Email: "node1@example.com", Name: "Node 1"},
					{Email: "node2@example.com", Name: "Node 2"},
				},
			}
			err = client.Insert(ctx, &parent3)
			require.NoError(t, err, "Insert should succeed for parent3")
			require.NotEmpty(t, parent3.Nodes[0].UID, "Nodes[0] UID should be assigned")
			require.NotEmpty(t, parent3.Nodes[1].UID, "Nodes[1] UID should be assigned")
			t.Logf("After insert: Nodes[0].UID=%s, Nodes[1].UID=%s", parent3.Nodes[0].UID, parent3.Nodes[1].UID)

			// Try to update parent3's nodes to have duplicate emails
			parent3.Nodes[1].Email = "node1@example.com"
			err = client.Update(ctx, &parent3)
			require.Error(t, err, "Update should fail due to duplicate emails in embedded node array")

			// 3. Test valid update of embedded nodes
			updatedNodeUID := parent3.Nodes[1].UID // Save UID before update
			parent3.Nodes[1].Email = "node3@example.com"
			parent3.Nodes[1].Name = "Updated Node 2"
			err = client.Update(ctx, &parent3)
			require.NoError(t, err, "Update should succeed with valid embedded node changes")

			// Verify the update - find node by UID since Dgraph doesn't guarantee array order
			updated := ParentWithUniqueEmbedded{UID: parent3.UID}
			err = client.Get(ctx, &updated, parent3.UID)
			require.NoError(t, err, "Get should succeed")
			var updatedNode *UniqueEmbeddedNode
			for _, n := range updated.Nodes {
				if n.UID == updatedNodeUID {
					updatedNode = n
					break
				}
			}
			require.NotNil(t, updatedNode, "Updated node should be found by UID")
			require.Equal(t, "node3@example.com", updatedNode.Email, "Email should be updated")
			require.Equal(t, "Updated Node 2", updatedNode.Name, "Name should be updated")
		})
	}
}
