/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type UpsertTestEntity struct {
	Name        string    `json:"name,omitempty" dgraph:"index=exact upsert"`
	AnotherName string    `json:"anotherName,omitempty" dgraph:"index=exact upsert"`
	Description string    `json:"description,omitempty" dgraph:"index=term"`
	CreatedAt   time.Time `json:"createdAt,omitzero"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func TestClientUpsert(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "InsertWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "InsertWithDgraphURI",
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

			t.Run("basic upsert", func(t *testing.T) {
				entity := UpsertTestEntity{
					Name:        "Test Entity", // This is the upsert field
					Description: "This is a test entity for the Upsert method",
					CreatedAt:   time.Date(2021, 6, 9, 17, 22, 33, 0, time.UTC),
				}

				ctx := context.Background()
				err := client.Upsert(ctx, &entity)
				require.NoError(t, err, "Upsert should succeed")
				require.NotEmpty(t, entity.UID, "UID should be assigned")

				uid := entity.UID
				err = client.Get(ctx, &entity, uid)
				require.NoError(t, err, "Get should succeed")
				require.Equal(t, "Test Entity", entity.Name, "Name should match")
				require.Equal(t, "This is a test entity for the Upsert method", entity.Description, "Description should match")

				newTime := time.Now().UTC().Truncate(time.Second)
				entity = UpsertTestEntity{
					Name:        "Test Entity", // This is the upsert field
					Description: "Updated description",
					CreatedAt:   newTime,
				}

				err = client.Upsert(ctx, &entity)
				require.NoError(t, err, "Upsert should succeed")

				uid = entity.UID
				err = client.Get(ctx, &entity, uid)
				require.NoError(t, err, "Get should succeed")
				require.Equal(t, "Test Entity", entity.Name, "Name should match")
				require.Equal(t, "Updated description", entity.Description, "Description should match")
				require.Equal(t, newTime, entity.CreatedAt, "CreatedAt should match")

				var entities []UpsertTestEntity
				err = client.Query(ctx, UpsertTestEntity{}).Nodes(&entities)
				require.NoError(t, err, "Query should succeed")
				require.Len(t, entities, 1, "There should only be one entity")
			})
			t.Run("upsert with predicate", func(t *testing.T) {

				ctx := context.Background()
				require.NoError(t, client.DropAll(ctx), "Drop all should succeed")

				entity := UpsertTestEntity{
					AnotherName: "Test Entity", // This is another upsert field, we have to define it to the call to upsert
					Description: "This is a test entity for the Upsert method",
					CreatedAt:   time.Date(2021, 6, 9, 17, 22, 33, 0, time.UTC),
				}

				err := client.Upsert(ctx, &entity, "anotherName")
				require.NoError(t, err, "Upsert should succeed")
				require.NotEmpty(t, entity.UID, "UID should be assigned")

				uid := entity.UID
				err = client.Get(ctx, &entity, uid)
				require.NoError(t, err, "Get should succeed")
				require.Equal(t, "Test Entity", entity.AnotherName, "AnotherName should match")
				require.Equal(t, "This is a test entity for the Upsert method", entity.Description, "Description should match")

				newTime := time.Now().UTC().Truncate(time.Second)
				entity = UpsertTestEntity{
					AnotherName: "Test Entity",
					Description: "Updated description",
					CreatedAt:   newTime,
				}

				err = client.Upsert(ctx, &entity, "anotherName")
				require.NoError(t, err, "Upsert should succeed")

				uid = entity.UID
				err = client.Get(ctx, &entity, uid)
				require.NoError(t, err, "Get should succeed")
				require.Equal(t, "Test Entity", entity.AnotherName, "AnotherName should match")
				require.Equal(t, "Updated description", entity.Description, "Description should match")
				require.Equal(t, newTime, entity.CreatedAt, "CreatedAt should match")

				var entities []UpsertTestEntity
				err = client.Query(ctx, UpsertTestEntity{}).Nodes(&entities)
				require.NoError(t, err, "Query should succeed")
				require.Len(t, entities, 1, "There should only be one entity")
			})
		})
	}
}

func TestClientUpsertSlice(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "UpsertSliceWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "UpsertSliceWithDgraphURI",
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
			if strings.HasPrefix(tc.uri, "dgraph://") {
				t.Skipf("Skipping %s: Dgraph URI not supported for upserting slices", tc.name)
				return
			}

			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			ctx := context.Background()
			require.NoError(t, client.DropAll(ctx), "Drop all should succeed")

			entities := []*UpsertTestEntity{
				{
					Name:        "Test Entity 1",
					Description: "This is a test entity for the Upsert method 1",
					CreatedAt:   time.Date(2021, 6, 9, 17, 22, 33, 0, time.UTC),
				},
				{
					Name:        "Test Entity 2",
					Description: "This is a test entity for the Upsert method 2",
					CreatedAt:   time.Date(2021, 6, 9, 17, 22, 34, 0, time.UTC),
				},
			}

			err := client.Upsert(ctx, &entities) // Here, no need to pass address of entities, but we handle it
			require.NoError(t, err, "Upsert should succeed")

			var entities2 []UpsertTestEntity
			err = client.Query(ctx, UpsertTestEntity{}).Nodes(&entities2)
			require.NoError(t, err, "Query should succeed")
			require.Len(t, entities2, 2, "There should be two entities")

			findMatchingEntity := func(entities []UpsertTestEntity, name string) *UpsertTestEntity {
				for i := range entities {
					if entities[i].Name == name {
						return &entities[i]
					}
				}
				return nil
			}

			// Check first entity
			entity1 := findMatchingEntity(entities2, entities[0].Name)
			require.NotNil(t, entity1, "Should find entity with name %s", entities[0].Name)
			require.Equal(t, entities[0].Description, entity1.Description, "Description should match")
			require.Equal(t, entities[0].CreatedAt, entity1.CreatedAt, "CreatedAt should match")

			// Check second entity
			entity2 := findMatchingEntity(entities2, entities[1].Name)
			require.NotNil(t, entity2, "Should find entity with name %s", entities[1].Name)
			require.Equal(t, entities[1].Description, entity2.Description, "Description should match")
			require.Equal(t, entities[1].CreatedAt, entity2.CreatedAt, "CreatedAt should match")

			entities[0].Name = "Test Entity 1"
			entities[0].Description = "Updated description"
			entities[1].Name = "Test Entity 2"
			entities[1].Description = "Updated description"
			err = client.Upsert(ctx, entities)
			require.NoError(t, err, "Upsert should succeed")

			var entities3 []UpsertTestEntity
			err = client.Query(ctx, UpsertTestEntity{}).Nodes(&entities3)
			require.NoError(t, err, "Query should succeed")
			require.Len(t, entities3, 2, "There should be two entities")

			entity1 = findMatchingEntity(entities3, entities[0].Name)
			require.NotNil(t, entity1, "Should find entity with name %s", entities[0].Name)
			require.Equal(t, entities[0].Description, entity1.Description, "Description should match")
			require.Equal(t, entities[0].CreatedAt, entity1.CreatedAt, "CreatedAt should match")

			entity2 = findMatchingEntity(entities3, entities[1].Name)
			require.NotNil(t, entity2, "Should find entity with name %s", entities[1].Name)
			require.Equal(t, entities[1].Description, entity2.Description, "Description should match")
			require.Equal(t, entities[1].CreatedAt, entity2.CreatedAt, "CreatedAt should match")
		})
	}
}

type EmbeddedUpsertEntity struct {
	Name        string    `json:"name,omitempty" dgraph:"index=exact upsert"`
	Description string    `json:"description,omitempty" dgraph:"index=term"`
	CreatedAt   time.Time `json:"createdAt,omitzero"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

type OuterUpsertEntity struct {
	Title  string                `json:"title,omitempty"`
	Entity *EmbeddedUpsertEntity `json:"entity"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func TestEmbeddedUpsert(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "EmbeddedUpsertWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "EmbeddedUpsertWithDgraphURI",
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

			// First upsert - creates new entity with embedded entity
			firstEntity := OuterUpsertEntity{
				Title: "First Title",
				Entity: &EmbeddedUpsertEntity{
					Name:        "Unique Embedded Name",
					Description: "First description",
					CreatedAt:   time.Date(2021, 6, 9, 17, 22, 33, 0, time.UTC),
				},
			}

			err := client.Upsert(ctx, &firstEntity)
			require.NoError(t, err, "First upsert should succeed")
			require.NotEmpty(t, firstEntity.UID, "Outer UID should be assigned")
			require.NotEmpty(t, firstEntity.Entity.UID, "Embedded UID should be assigned")

			firstUID := firstEntity.UID
			firstEmbeddedUID := firstEntity.Entity.UID

			// Verify first entity was created correctly
			retrieved := OuterUpsertEntity{}
			err = client.Get(ctx, &retrieved, firstUID)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, "First Title", retrieved.Title, "Title should match")
			require.Equal(t, "Unique Embedded Name", retrieved.Entity.Name, "Embedded name should match")
			require.Equal(t, "First description", retrieved.Entity.Description, "Embedded description should match")

			// Second upsert - embedded entity with same upsert key should be UPDATED, not created
			secondEntity := OuterUpsertEntity{
				Title: "Second Title",
				Entity: &EmbeddedUpsertEntity{
					Name:        "Unique Embedded Name", // Same name - should trigger upsert on embedded entity
					Description: "Updated description",
					CreatedAt:   time.Date(2022, 7, 10, 18, 30, 45, 0, time.UTC),
				},
			}

			err = client.Upsert(ctx, &secondEntity)
			require.NoError(t, err, "Second upsert should succeed")
			require.NotEmpty(t, secondEntity.UID, "Outer UID should be assigned")
			require.NotEmpty(t, secondEntity.Entity.UID, "Embedded UID should be assigned")

			// The embedded entity should have the SAME UID (upserted, not created new)
			require.Equal(t, firstEmbeddedUID, secondEntity.Entity.UID, "Embedded UID should be the same (upserted)")

			// Verify only ONE embedded entity exists (it was upserted, not duplicated)
			var embeddedEntities []EmbeddedUpsertEntity
			err = client.Query(ctx, EmbeddedUpsertEntity{}).Nodes(&embeddedEntities)
			require.NoError(t, err, "Query embedded entities should succeed")
			require.Len(t, embeddedEntities, 1, "There should be only one embedded entity (upserted)")

			// Verify the embedded entity has the updated description
			require.Equal(t, "Unique Embedded Name", embeddedEntities[0].Name, "Name should match")
			require.Equal(t, "Updated description", embeddedEntities[0].Description, "Description should be updated")

			// Verify there are two outer entities (they have different titles, so not upserted)
			var outerEntities []OuterUpsertEntity
			err = client.Query(ctx, OuterUpsertEntity{}).Nodes(&outerEntities)
			require.NoError(t, err, "Query outer entities should succeed")
			require.Len(t, outerEntities, 2, "There should be two outer entities")
		})
	}
}
