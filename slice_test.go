/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Genre is a simple struct used as a value-type slice element.
type Genre struct {
	Name string `json:"name,omitempty" dgraph:"index=exact"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

// MovieWithValueSlice uses []Genre (value-type slice) for its genres field.
type MovieWithValueSlice struct {
	Title  string  `json:"title,omitempty" dgraph:"index=exact"`
	Genres []Genre `json:"genres,omitempty"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

// MovieWithPointerSlice uses []*Genre (pointer-type slice) for its genres field.
type MovieWithPointerSlice struct {
	Title  string   `json:"title,omitempty" dgraph:"index=exact"`
	Genres []*Genre `json:"genres,omitempty"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

// TestValueTypeSliceInsertAndQuery tests that []T (value-type slice) fields
// round-trip correctly through insert and query.
func TestValueTypeSliceInsertAndQuery(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "ValueTypeSliceWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "ValueTypeSliceWithDgraphURI",
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

			// Insert a movie with value-type []Genre slice
			movie := MovieWithValueSlice{
				Title: "The Matrix",
				Genres: []Genre{
					{Name: "Action"},
					{Name: "Sci-Fi"},
				},
			}

			err := client.Insert(ctx, &movie)
			require.NoError(t, err, "Insert with []Genre should succeed")
			require.NotEmpty(t, movie.UID, "Movie UID should be assigned")
			// Verify that nested Genre UIDs were assigned
			for i, g := range movie.Genres {
				require.NotEmpty(t, g.UID, "Genre[%d] UID should be assigned", i)
			}

			// Query the movie back and verify genres are populated
			var retrieved MovieWithValueSlice
			err = client.Get(ctx, &retrieved, movie.UID)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, "The Matrix", retrieved.Title, "Title should match")
			require.Len(t, retrieved.Genres, 2, "Should have 2 genres")

			genreNames := []string{retrieved.Genres[0].Name, retrieved.Genres[1].Name}
			assert.ElementsMatch(t, []string{"Action", "Sci-Fi"}, genreNames,
				"Genre names should match")

			// Also verify via Query
			var results []MovieWithValueSlice
			err = client.Query(ctx, MovieWithValueSlice{}).
				Filter(`eq(title, "The Matrix")`).
				Nodes(&results)
			require.NoError(t, err, "Query should succeed")
			require.Len(t, results, 1, "Should find 1 movie")
			require.Len(t, results[0].Genres, 2, "Queried movie should have 2 genres")
		})
	}
}

// TestPointerTypeSliceInsertAndQuery tests that []*T (pointer-type slice) fields
// round-trip correctly through insert and query (baseline for comparison).
func TestPointerTypeSliceInsertAndQuery(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "PointerTypeSliceWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "PointerTypeSliceWithDgraphURI",
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

			// Insert a movie with pointer-type []*Genre slice
			movie := MovieWithPointerSlice{
				Title: "Inception",
				Genres: []*Genre{
					{Name: "Thriller"},
					{Name: "Sci-Fi"},
				},
			}

			err := client.Insert(ctx, &movie)
			require.NoError(t, err, "Insert with []*Genre should succeed")
			require.NotEmpty(t, movie.UID, "Movie UID should be assigned")
			for i, g := range movie.Genres {
				require.NotEmpty(t, g.UID, "Genre[%d] UID should be assigned", i)
			}

			// Query the movie back and verify genres are populated
			var retrieved MovieWithPointerSlice
			err = client.Get(ctx, &retrieved, movie.UID)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, "Inception", retrieved.Title, "Title should match")
			require.Len(t, retrieved.Genres, 2, "Should have 2 genres")

			genreNames := []string{retrieved.Genres[0].Name, retrieved.Genres[1].Name}
			assert.ElementsMatch(t, []string{"Thriller", "Sci-Fi"}, genreNames,
				"Genre names should match")

			// Also verify via Query
			var results []MovieWithPointerSlice
			err = client.Query(ctx, MovieWithPointerSlice{}).
				Filter(`eq(title, "Inception")`).
				Nodes(&results)
			require.NoError(t, err, "Query should succeed")
			require.Len(t, results, 1, "Should find 1 movie")
			require.Len(t, results[0].Genres, 2, "Queried movie should have 2 genres")
		})
	}
}

// TestValueTypeSliceParity compares behavior of []T and []*T to confirm they
// produce equivalent results when round-tripped through dgdao.
func TestValueTypeSliceParity(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "SliceParityWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "SliceParityWithDgraphURI",
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

			// Insert via value-type slice
			valueMovie := MovieWithValueSlice{
				Title: "Value Movie",
				Genres: []Genre{
					{Name: "Drama"},
					{Name: "Comedy"},
					{Name: "Romance"},
				},
			}
			err := client.Insert(ctx, &valueMovie)
			require.NoError(t, err, "Insert with []Genre should succeed")
			require.NotEmpty(t, valueMovie.UID)

			// Insert via pointer-type slice
			ptrMovie := MovieWithPointerSlice{
				Title: "Pointer Movie",
				Genres: []*Genre{
					{Name: "Horror"},
					{Name: "Mystery"},
					{Name: "Thriller"},
				},
			}
			err = client.Insert(ctx, &ptrMovie)
			require.NoError(t, err, "Insert with []*Genre should succeed")
			require.NotEmpty(t, ptrMovie.UID)

			// Query back both
			var valueResult MovieWithValueSlice
			err = client.Get(ctx, &valueResult, valueMovie.UID)
			require.NoError(t, err, "Get value movie should succeed")

			var ptrResult MovieWithPointerSlice
			err = client.Get(ctx, &ptrResult, ptrMovie.UID)
			require.NoError(t, err, "Get pointer movie should succeed")

			// Both should have 3 genres
			require.Len(t, valueResult.Genres, 3,
				"Value-type movie should have 3 genres")
			require.Len(t, ptrResult.Genres, 3,
				"Pointer-type movie should have 3 genres")

			// Verify all genre UIDs are non-empty
			for i, g := range valueResult.Genres {
				assert.NotEmpty(t, g.UID, "Value genre[%d] should have UID", i)
				assert.NotEmpty(t, g.Name, "Value genre[%d] should have Name", i)
			}
			for i, g := range ptrResult.Genres {
				assert.NotEmpty(t, g.UID, "Pointer genre[%d] should have UID", i)
				assert.NotEmpty(t, g.Name, "Pointer genre[%d] should have Name", i)
			}

			// Verify the genre names are as expected
			valueGenreNames := make([]string, len(valueResult.Genres))
			for i, g := range valueResult.Genres {
				valueGenreNames[i] = g.Name
			}
			assert.ElementsMatch(t, []string{"Drama", "Comedy", "Romance"},
				valueGenreNames, "Value-type genre names should match")

			ptrGenreNames := make([]string, len(ptrResult.Genres))
			for i, g := range ptrResult.Genres {
				ptrGenreNames[i] = g.Name
			}
			assert.ElementsMatch(t, []string{"Horror", "Mystery", "Thriller"},
				ptrGenreNames, "Pointer-type genre names should match")
		})
	}
}

// TestValueTypeSliceUpdate tests that updating a struct with []T slice works.
func TestValueTypeSliceUpdate(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "ValueTypeSliceUpdateWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "ValueTypeSliceUpdateWithDgraphURI",
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

			// Insert initial movie with value-type genres
			movie := MovieWithValueSlice{
				Title: "Test Movie",
				Genres: []Genre{
					{Name: "Action"},
				},
			}
			err := client.Insert(ctx, &movie)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, movie.UID)

			// Update: add a new genre
			movie.Genres = append(movie.Genres, Genre{Name: "Adventure"})
			err = client.Update(ctx, &movie)
			require.NoError(t, err, "Update with additional genre should succeed")

			// Verify the update
			var updated MovieWithValueSlice
			err = client.Get(ctx, &updated, movie.UID)
			require.NoError(t, err, "Get should succeed after update")
			require.Len(t, updated.Genres, 2, "Should have 2 genres after update")

			genreNames := make([]string, len(updated.Genres))
			for i, g := range updated.Genres {
				genreNames[i] = g.Name
			}
			assert.ElementsMatch(t, []string{"Action", "Adventure"}, genreNames,
				"Genre names should match after update")
		})
	}
}

// TestValueTypeSliceEmpty tests behavior with empty []T slices.
func TestValueTypeSliceEmpty(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "EmptyValueSliceWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "EmptyValueSliceWithDgraphURI",
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

			// Insert a movie with no genres (nil slice)
			movie := MovieWithValueSlice{
				Title: "No Genre Movie",
			}
			err := client.Insert(ctx, &movie)
			require.NoError(t, err, "Insert with nil genres should succeed")
			require.NotEmpty(t, movie.UID)

			// Query back
			var retrieved MovieWithValueSlice
			err = client.Get(ctx, &retrieved, movie.UID)
			require.NoError(t, err, "Get should succeed")
			require.Equal(t, "No Genre Movie", retrieved.Title)
			assert.Empty(t, retrieved.Genres, "Genres should be empty")
		})
	}
}
