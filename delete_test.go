/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/stretchr/testify/require"
)

func TestClientDelete(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "DeleteWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "DeleteWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("DGDAO_TEST_ADDR"),
			skip: os.Getenv("DGDAO_TEST_ADDR") == "",
		},
	}

	createTestEntities := func() []*TestEntity {
		entities := []*TestEntity{}
		for i := range 10 {
			entities = append(entities, &TestEntity{
				Name:        fmt.Sprintf("Test Entity %d", i),
				Description: fmt.Sprintf("This is a test entity (%d) for the Update method", i),
				CreatedAt:   time.Now(),
			})
		}
		return entities
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
				return
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			entities := createTestEntities()

			ctx := context.Background()
			err := client.Insert(ctx, entities)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, entities[0].UID, "UID should be assigned")

			// Get the UIDs of the first 5 entities
			uids := make([]string, 5)
			for i, entity := range entities[:5] {
				uids[i] = entity.UID
			}

			err = client.Delete(ctx, uids)
			require.NoError(t, err, "Delete should succeed")

			var result []TestEntity
			err = client.Query(ctx, TestEntity{}).Nodes(&result)
			require.NoError(t, err, "Query should succeed")
			require.Len(t, result, 5, "Should have 5 entities remaining")
		})
	}
}

func TestDeletePredicate(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "DeletePredicateWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "DeletePredicateWithDgraphURI",
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

			// Create multiple entities to test predicate deletion across all nodes
			entities := []*TestEntity{
				{
					Name:        "Test Entity 1",
					Description: "This is test entity 1",
					CreatedAt:   time.Now(),
				},
				{
					Name:        "Test Entity 2",
					Description: "This is test entity 2",
					CreatedAt:   time.Now(),
				},
				{
					Name:        "Test Entity 3",
					Description: "This is test entity 3",
					CreatedAt:   time.Now(),
				},
			}

			ctx := context.Background()
			err := client.Insert(ctx, entities)
			require.NoError(t, err, "Insert should succeed")
			for _, entity := range entities {
				require.NotEmpty(t, entity.UID, "UID should be assigned")
			}

			dgClient, cleanup, err := client.DgraphClient()
			require.NoError(t, err)
			defer cleanup()

			const pred = "description"

			// Delete the description predicate from all nodes that have it
			// First query to find all entities with the predicate
			queryEntities := `{
				entities(func: has(` + pred + `)) {
					uid
				}
			}`

			_, err = dgClient.NewTxn().Query(ctx, queryEntities)
			require.NoError(t, err, "Query should succeed")

			// Parse the response to extract UIDs (in a real scenario, you'd parse JSON)
			// For this test, we'll build delete mutations for all entities with the predicate
			// Build delete mutation for all entities
			var deleteMutations []string
			for _, entity := range entities {
				deleteMutations = append(deleteMutations, `<`+entity.UID+`> <`+pred+`> * .`)
			}

			// Combine all delete mutations into one
			deleteMutation := strings.Join(deleteMutations, "\n")

			req := &api.Request{
				Mutations: []*api.Mutation{
					{
						DelNquads: []byte(deleteMutation),
					},
				},
				CommitNow: true,
			}

			_, err = dgClient.NewTxn().Do(ctx, req)
			if err != nil {
				log.Fatalf("delete predicate data failed: %v", err)
			}

			// Verify all entities still exist but description predicate is gone
			for i, originalEntity := range entities {
				var updatedEntity TestEntity
				err = client.Get(ctx, &updatedEntity, originalEntity.UID)
				require.NoError(t, err, "Get should succeed for entity %d", i+1)
				require.Empty(t, updatedEntity.Description, "Description should be empty for entity %d", i+1)
				require.NotEmpty(t, updatedEntity.Name, "Name should still exist for entity %d", i+1)
				require.NotEmpty(t, updatedEntity.UID, "UID should still exist for entity %d", i+1)
			}
		})
	}
}
