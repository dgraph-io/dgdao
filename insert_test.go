/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/dgdao"
)

// TestEntity is a test struct used for Insert tests
type TestEntity struct {
	UID         string    `json:"uid,omitempty"`
	Name        string    `json:"name,omitempty" dgraph:"index=term,exact unique"`
	Description string    `json:"description,omitempty" dgraph:"index=term"`
	CreatedAt   time.Time `json:"createdAt,omitempty"`
	DType       []string  `json:"dgraph.type,omitempty"`
}

func TestClientInsert(t *testing.T) {

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

			entity := TestEntity{
				Name:        "Test Entity",
				Description: "This is a test entity for the Insert method",
				CreatedAt:   time.Now(),
			}

			ctx := context.Background()
			err := client.Insert(ctx, &entity)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, entity.UID, "UID should be assigned")

			uid := entity.UID
			err = client.Get(ctx, &entity, uid)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, entity.Name, "Test Entity", "Name should match")
			require.Equal(t, entity.Description, "This is a test entity for the Insert method", "Description should match")

			// Try to insert the same entity again, should fail due to unique constraint
			entity = TestEntity{
				Name:        "Test Entity",
				Description: "This is a test entity for the Insert method 2",
				CreatedAt:   time.Now(),
			}
			err = client.Insert(ctx, &entity)
			require.Error(t, err, "Insert should fail")
			if strings.HasPrefix(tc.uri, "file://") {
				var uniqueErr *dgdao.UniqueError
				require.True(t, errors.As(err, &uniqueErr), "Error should be a UniqueError")
				require.Equal(t, uid, uniqueErr.UID, "UID should match")
			}

			var entities []TestEntity
			err = client.Query(ctx, TestEntity{}).Nodes(&entities)
			require.NoError(t, err, "Query should succeed")
			require.Len(t, entities, 1, "There should only be one entity")
		})
	}
}

type OuterTestEntity struct {
	Name   string      `json:"name,omitempty" dgraph:"index=exact unique"`
	Entity *TestEntity `json:"entity"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func TestEmbeddedInsert(t *testing.T) {
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

			timestamp := time.Now().UTC().Truncate(time.Second)
			entity := OuterTestEntity{
				Name: "Test Outer Entity",
				Entity: &TestEntity{
					Name:        "Test Inner Entity",
					Description: "This is a test entity for the Insert method",
					CreatedAt:   timestamp,
				},
			}

			ctx := context.Background()
			err := client.Insert(ctx, &entity)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, entity.UID, "UID should be assigned")

			uid := entity.UID
			entity = OuterTestEntity{}
			err = client.Get(ctx, &entity, uid)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, "Test Outer Entity", entity.Name, "Name should match")
			require.Equal(t, "Test Inner Entity", entity.Entity.Name, "Entity.Name should match")
			require.Equal(t, "This is a test entity for the Insert method",
				entity.Entity.Description, "Entity.Description should match")
			require.Equal(t, timestamp, entity.Entity.CreatedAt, "Entity.CreatedAt should match")
		})
	}
}

func TestEmbeddedUniqueConstraintViolation(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "EmbeddedUniqueViolationWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "EmbeddedUniqueViolationWithDgraphURI",
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

			// Insert first entity with embedded unique entity
			firstEntity := OuterTestEntity{
				Name: "First Outer Entity",
				Entity: &TestEntity{
					Name:        "Unique Inner Name",
					Description: "First description",
					CreatedAt:   time.Now(),
				},
			}

			err := client.Insert(ctx, &firstEntity)
			require.NoError(t, err, "First insert should succeed")
			require.NotEmpty(t, firstEntity.UID, "First UID should be assigned")
			require.NotEmpty(t, firstEntity.Entity.UID, "Embedded entity UID should be assigned")

			// Try to insert second entity with same embedded unique name
			secondEntity := OuterTestEntity{
				Name: "Second Outer Entity",
				Entity: &TestEntity{
					Name:        "Unique Inner Name", // Same name - should violate unique constraint
					Description: "Second description",
					CreatedAt:   time.Now(),
				},
			}

			err = client.Insert(ctx, &secondEntity)
			require.Error(t, err, "Second insert should fail due to unique constraint violation in embedded entity")

			// Verify the error is a UniqueError
			var uniqueErr *dgdao.UniqueError
			require.True(t, errors.As(err, &uniqueErr), "Error should be a UniqueError")
			// UID is only available for file URIs (embedded engine checks the database)
			// Dgraph errors don't include the UID of the existing entity
			if strings.HasPrefix(tc.uri, "file://") {
				require.Equal(t, firstEntity.Entity.UID, uniqueErr.UID, "UID should match the first embedded entity")
			}

			// Verify only the first entity exists
			var entities []OuterTestEntity
			err = client.Query(ctx, OuterTestEntity{}).Nodes(&entities)
			require.NoError(t, err, "Query should succeed")
			require.Len(t, entities, 1, "There should only be one outer entity")
			require.Equal(t, "First Outer Entity", entities[0].Name, "Should be the first entity")
			require.Equal(t, "Unique Inner Name", entities[0].Entity.Name, "Embedded entity name should match")
		})
	}
}

func TestClientInsertMultipleEntities(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "InsertMultipleWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "InsertMultipleWithDgraphURI",
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

			// Note the `*TestEntity`, the elements in the slice must be pointers
			entities := []*TestEntity{
				{
					Name:        "Entity 1",
					Description: "First test entity",
					CreatedAt:   time.Now().Add(-1 * time.Hour),
				},
				{
					Name:        "Entity 2",
					Description: "Second test entity",
					CreatedAt:   time.Now(),
				},
			}

			ctx := context.Background()
			err := client.Insert(ctx, entities)
			require.NoError(t, err, "Insert should succeed")

			var result []TestEntity
			err = client.Query(ctx, TestEntity{}).OrderDesc("createdAt").First(1).Nodes(&result)
			require.NoError(t, err, "Query should succeed")
			assert.Len(t, result, 1, "Should have found one entity")
			assert.Equal(t, entities[1].Name, result[0].Name, "Name should match")
		})
	}
}

type Person struct {
	Name    string    `json:"name,omitempty" dgraph:"index=term"`
	Friends []*Person `json:"friends,omitempty"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func TestDepthQuery(t *testing.T) {

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

	createPerson := func() Person {
		return Person{
			Name: "Alice",
			Friends: []*Person{
				{
					Name: "Bob",
					Friends: []*Person{
						{
							Name: "Charles",
						},
						{
							Name: "David",
							Friends: []*Person{
								{
									Name: "Eve",
									Friends: []*Person{
										{
											Name: "Frank",
										},
									},
								},
								{
									Name: "George",
								},
							},
						},
					},
				},
			},
		}
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
			person := createPerson()
			err := client.Insert(ctx, &person)
			require.NoError(t, err, "Insert should succeed")

			var result []Person
			err = client.Query(ctx, Person{}).Filter(`eq(name, "Alice")`).Nodes(&result)
			require.NoError(t, err, "Query should succeed")
			assert.Equal(t, person.Name, result[0].Name, "Name should match")

			verifyPersonStructure(t, &person, &result[0])
		})
	}
}

func verifyPersonStructure(t *testing.T, expected *Person, actual *Person) {
	t.Helper()
	require.NotNil(t, actual, "Person should not be nil")
	assert.Equal(t, expected.Name, actual.Name, "Name should match")

	if expected.Friends == nil {
		assert.Empty(t, actual.Friends, "Should have no friends")
		return
	}

	require.Len(t, actual.Friends, len(expected.Friends),
		"%s should have %d friends", expected.Name, len(expected.Friends))

	// Create a map of expected friends by name for easier lookup
	expectedFriends := make(map[string]*Person)
	for _, friend := range expected.Friends {
		expectedFriends[friend.Name] = friend
	}

	// Verify each actual friend
	for _, actualFriend := range actual.Friends {
		expectedFriend, ok := expectedFriends[actualFriend.Name]
		require.True(t, ok, "%s should have a friend named %s",
			expected.Name, actualFriend.Name)

		// Recursively verify this friend's structure
		verifyPersonStructure(t, expectedFriend, actualFriend)
	}
}

func TestClientInsertMultipleEntitiesWithEmbeddedStructs(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "InsertMultipleEmbeddedWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "InsertMultipleEmbeddedWithDgraphURI",
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

			timestamp := time.Now().UTC().Truncate(time.Second)

			// Note: elements in the slice must be pointers
			entities := []*OuterTestEntity{
				{
					Name: "Outer Entity 1",
					Entity: &TestEntity{
						Name:        "Inner Entity 1",
						Description: "First embedded entity",
						CreatedAt:   timestamp.Add(-1 * time.Hour),
					},
				},
				{
					Name: "Outer Entity 2",
					Entity: &TestEntity{
						Name:        "Inner Entity 2",
						Description: "Second embedded entity",
						CreatedAt:   timestamp,
					},
				},
				{
					Name: "Outer Entity 3",
					Entity: &TestEntity{
						Name:        "Inner Entity 3",
						Description: "Third embedded entity",
						CreatedAt:   timestamp.Add(1 * time.Hour),
					},
				},
			}

			ctx := context.Background()
			err := client.Insert(ctx, entities)
			require.NoError(t, err, "Insert should succeed")

			// Verify all entities got UIDs assigned
			for i, entity := range entities {
				require.NotEmpty(t, entity.UID, "UID should be assigned for entity %d", i)
				require.NotEmpty(t, entity.Entity.UID, "Embedded entity UID should be assigned for entity %d", i)
			}

			// Verify we can retrieve each entity with its embedded struct
			for _, original := range entities {
				var retrieved OuterTestEntity
				err = client.Get(ctx, &retrieved, original.UID)
				require.NoError(t, err, "Get should succeed for %s", original.Name)
				assert.Equal(t, original.Name, retrieved.Name, "Name should match")
				require.NotNil(t, retrieved.Entity, "Embedded entity should not be nil")
				assert.Equal(t, original.Entity.Name, retrieved.Entity.Name, "Embedded entity name should match")
				assert.Equal(t, original.Entity.Description, retrieved.Entity.Description,
					"Embedded entity description should match")
			}

			// Query all outer entities and verify count
			var results []OuterTestEntity
			err = client.Query(ctx, OuterTestEntity{}).Nodes(&results)
			require.NoError(t, err, "Query should succeed")
			assert.Len(t, results, 3, "Should have found all three entities")
		})
	}
}
