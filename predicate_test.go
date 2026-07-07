/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NOTE: These tests exercise the predicate= tag through dgdao's client API.
// They depend on the dgman fork (mlwelles/dgman) containing the predicate= fixes
// for both the write path (filterStruct using schema.Predicate as map key) and
// the read path (remapping JSON keys from predicate names to json tag names).
// Until the dgman fork fixes land, these tests will fail because:
//   - MutateBasic writes data under the json tag name instead of the predicate name
//   - Query/Get returns zero values for fields where predicate != json tag

// PredicateFilm is a test struct where the dgraph predicate name differs from
// the json tag name. This exercises the predicate= fix in dgman.
type PredicateFilm struct {
	Title       string    `json:"title,omitempty" dgraph:"predicate=film_title index=exact unique"`
	ReleaseDate time.Time `json:"releaseDate,omitzero" dgraph:"predicate=release_date index=day"`
	Rating      float64   `json:"rating,omitempty" dgraph:"predicate=film_rating index=float"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

// PredicateBook and PredicateAuthor test forward and reverse edges using the
// predicate= tag. This mirrors the pattern used by dgdaoGen where:
//   - PredicateBook has a forward edge: predicate=written_by reverse
//   - PredicateAuthor has a reverse edge: predicate=~written_by reverse
//
// The forward edge creates an @reverse index in Dgraph, and the reverse edge
// declares a managed reverse that dgman expands in queries automatically.
type PredicateBook struct {
	Title  string           `json:"bookTitle,omitempty" dgraph:"predicate=book_title index=exact unique"`
	Year   int              `json:"bookYear,omitempty" dgraph:"predicate=book_year index=int"`
	Author *PredicateAuthor `json:"author,omitempty" dgraph:"predicate=written_by reverse"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

type PredicateAuthor struct {
	Name  string          `json:"authorName,omitempty" dgraph:"predicate=author_name index=exact unique"`
	Books []PredicateBook `json:"books,omitempty" dgraph:"predicate=~written_by reverse"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

// TestPredicateInsertAndGet tests that Insert + Get round-trips correctly
// when predicate= differs from the json tag.
func TestPredicateInsertAndGet(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "PredicateInsertGetWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "PredicateInsertGetWithDgraphURI",
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
			releaseDate := time.Date(1999, 3, 31, 0, 0, 0, 0, time.UTC)

			film := PredicateFilm{
				Title:       "The Matrix",
				ReleaseDate: releaseDate,
				Rating:      8.7,
			}

			err := client.Insert(ctx, &film)
			require.NoError(t, err, "Insert should succeed")
			require.NotEmpty(t, film.UID, "UID should be assigned")

			// Get the film back by UID
			var retrieved PredicateFilm
			err = client.Get(ctx, &retrieved, film.UID)
			require.NoError(t, err, "Get should succeed")

			// These assertions verify the predicate= fix: data stored under
			// the predicate name (film_title, release_date, film_rating) should
			// be correctly mapped back to the json tag fields.
			assert.Equal(t, "The Matrix", retrieved.Title,
				"Title should round-trip correctly (predicate=film_title)")
			assert.Equal(t, releaseDate, retrieved.ReleaseDate,
				"ReleaseDate should round-trip correctly (predicate=release_date)")
			assert.Equal(t, 8.7, retrieved.Rating,
				"Rating should round-trip correctly (predicate=film_rating)")
		})
	}
}

// TestPredicateUpdate tests that Update works correctly with predicate= fields.
func TestPredicateUpdate(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "PredicateUpdateWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "PredicateUpdateWithDgraphURI",
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
			releaseDate := time.Date(1999, 3, 31, 0, 0, 0, 0, time.UTC)

			film := PredicateFilm{
				Title:       "The Matrix",
				ReleaseDate: releaseDate,
				Rating:      8.7,
			}

			err := client.Insert(ctx, &film)
			require.NoError(t, err, "Insert should succeed")

			// Update the rating
			film.Rating = 9.0
			err = client.Update(ctx, &film)
			require.NoError(t, err, "Update should succeed")

			var retrieved PredicateFilm
			err = client.Get(ctx, &retrieved, film.UID)
			require.NoError(t, err, "Get should succeed after update")
			assert.Equal(t, 9.0, retrieved.Rating,
				"Rating should be updated via predicate=film_rating")
			assert.Equal(t, "The Matrix", retrieved.Title,
				"Title should still be correct after update")
		})
	}
}

// TestPredicateUpsert tests that Upsert works correctly with predicate= fields.
func TestPredicateUpsert(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "PredicateUpsertWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "PredicateUpsertWithDgraphURI",
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
			releaseDate := time.Date(1999, 3, 31, 0, 0, 0, 0, time.UTC)

			// First upsert creates the node
			film := PredicateFilm{
				Title:       "The Matrix",
				ReleaseDate: releaseDate,
				Rating:      8.7,
			}

			err := client.Upsert(ctx, &film, "film_title")
			require.NoError(t, err, "Upsert (create) should succeed")
			require.NotEmpty(t, film.UID, "UID should be assigned")
			firstUID := film.UID

			// Second upsert updates the existing node
			film2 := PredicateFilm{
				Title:       "The Matrix",
				ReleaseDate: releaseDate,
				Rating:      9.1,
			}
			err = client.Upsert(ctx, &film2, "film_title")
			require.NoError(t, err, "Upsert (update) should succeed")
			assert.Equal(t, firstUID, film2.UID,
				"Upsert should reuse the same UID")

			// Verify the update
			var retrieved PredicateFilm
			err = client.Get(ctx, &retrieved, firstUID)
			require.NoError(t, err, "Get should succeed after upsert")
			assert.Equal(t, "The Matrix", retrieved.Title)
			assert.Equal(t, 9.1, retrieved.Rating,
				"Rating should be updated after upsert")
		})
	}
}

// TestPredicateQuery tests that Query with filters works correctly
// when predicates differ from json tags.
func TestPredicateQuery(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "PredicateQueryWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "PredicateQueryWithDgraphURI",
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

			// Insert multiple films with different release dates
			films := []*PredicateFilm{
				{
					Title:       "Film A",
					ReleaseDate: time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC),
					Rating:      7.5,
				},
				{
					Title:       "Film B",
					ReleaseDate: time.Date(1995, 6, 15, 0, 0, 0, 0, time.UTC),
					Rating:      8.0,
				},
				{
					Title:       "Film C",
					ReleaseDate: time.Date(2005, 12, 25, 0, 0, 0, 0, time.UTC),
					Rating:      9.0,
				},
			}
			err := client.Insert(ctx, films)
			require.NoError(t, err, "Insert films should succeed")

			// Query using predicate names (not json tag names) in filter.
			// The filter references release_date (the Dgraph predicate name).
			var results []PredicateFilm
			err = client.Query(ctx, PredicateFilm{}).
				Filter(`ge(release_date, "1990-01-01T00:00:00Z")`).
				Nodes(&results)
			require.NoError(t, err, "Query with predicate filter should succeed")
			require.Len(t, results, 2,
				"Should find 2 films with release_date >= 1990")

			titles := make([]string, len(results))
			for i, r := range results {
				titles[i] = r.Title
			}
			assert.ElementsMatch(t, []string{"Film B", "Film C"}, titles,
				"Should find the correct films")

			// Verify that all queried films have their predicate= fields populated
			for _, r := range results {
				assert.NotEmpty(t, r.Title, "Title should be populated")
				assert.False(t, r.ReleaseDate.IsZero(),
					"ReleaseDate should be populated (predicate=release_date)")
				assert.NotZero(t, r.Rating,
					"Rating should be populated (predicate=film_rating)")
			}
		})
	}
}

// TestPredicateReverseEdge tests that forward edges with predicate=<name>
// and reverse edges with predicate=~<name> work correctly together.
// This mirrors the pattern used by dgdaoGen (e.g. Film.genre / Genre.~genre).
func TestPredicateReverseEdge(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "PredicateReverseWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "PredicateReverseWithDgraphURI",
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

			// Create an author first
			author := &PredicateAuthor{
				Name: "Tolkien",
			}
			err := client.Insert(ctx, author)
			require.NoError(t, err, "Insert author should succeed")
			require.NotEmpty(t, author.UID, "Author UID should be assigned")

			// Create books with the forward edge to the author
			book1 := &PredicateBook{
				Title:  "The Hobbit",
				Year:   1937,
				Author: author,
			}
			book2 := &PredicateBook{
				Title:  "The Lord of the Rings",
				Year:   1954,
				Author: author,
			}

			err = client.Insert(ctx, book1)
			require.NoError(t, err, "Insert book1 should succeed")
			require.NotEmpty(t, book1.UID, "Book1 UID should be assigned")

			err = client.Insert(ctx, book2)
			require.NoError(t, err, "Insert book2 should succeed")
			require.NotEmpty(t, book2.UID, "Book2 UID should be assigned")

			// Get a book back and verify the forward edge (Author) is populated
			var gotBook PredicateBook
			err = client.Get(ctx, &gotBook, book1.UID)
			require.NoError(t, err, "Get book should succeed")

			assert.Equal(t, "The Hobbit", gotBook.Title,
				"Title should round-trip (predicate=book_title)")
			assert.Equal(t, 1937, gotBook.Year,
				"Year should round-trip (predicate=book_year)")
			require.NotNil(t, gotBook.Author,
				"Author forward edge should be populated (predicate=written_by)")
			assert.Equal(t, "Tolkien", gotBook.Author.Name,
				"Author name should round-trip (predicate=author_name)")

			// Get the author back and verify the reverse edge (Books) is populated
			var gotAuthor PredicateAuthor
			err = client.Get(ctx, &gotAuthor, author.UID)
			require.NoError(t, err, "Get author should succeed")

			assert.Equal(t, "Tolkien", gotAuthor.Name,
				"Author name should round-trip (predicate=author_name)")
			require.Len(t, gotAuthor.Books, 2,
				"Author should have 2 books via reverse edge (predicate=~written_by)")

			bookTitles := make(map[string]bool)
			for _, b := range gotAuthor.Books {
				bookTitles[b.Title] = true
			}
			assert.True(t, bookTitles["The Hobbit"],
				"Reverse edge should include The Hobbit")
			assert.True(t, bookTitles["The Lord of the Rings"],
				"Reverse edge should include The Lord of the Rings")
		})
	}
}

// TestPredicateReverseEdgeQuery tests querying entities that have reverse edges
// with predicate=~<name>, and verifies filters work on the predicate names.
func TestPredicateReverseEdgeQuery(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "PredicateReverseQueryWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "PredicateReverseQueryWithDgraphURI",
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

			// Create two authors
			tolkien := &PredicateAuthor{Name: "Tolkien"}
			asimov := &PredicateAuthor{Name: "Asimov"}

			err := client.Insert(ctx, tolkien)
			require.NoError(t, err)
			err = client.Insert(ctx, asimov)
			require.NoError(t, err)

			// Create books for each author
			books := []*PredicateBook{
				{Title: "The Hobbit", Year: 1937, Author: tolkien},
				{Title: "The Lord of the Rings", Year: 1954, Author: tolkien},
				{Title: "Foundation", Year: 1951, Author: asimov},
				{Title: "I, Robot", Year: 1950, Author: asimov},
				{Title: "The Caves of Steel", Year: 1954, Author: asimov},
			}
			err = client.Insert(ctx, books)
			require.NoError(t, err, "Insert books should succeed")

			// Query authors and verify reverse edges are populated
			var authors []PredicateAuthor
			err = client.Query(ctx, PredicateAuthor{}).
				Filter(`eq(author_name, "Tolkien")`).
				Nodes(&authors)
			require.NoError(t, err, "Query with author_name filter should succeed")
			require.Len(t, authors, 1, "Should find exactly 1 author named Tolkien")
			assert.Len(t, authors[0].Books, 2,
				"Tolkien should have 2 books via reverse edge")

			// Query Asimov and verify he has 3 books
			var asimovResult []PredicateAuthor
			err = client.Query(ctx, PredicateAuthor{}).
				Filter(`eq(author_name, "Asimov")`).
				Nodes(&asimovResult)
			require.NoError(t, err, "Query for Asimov should succeed")
			require.Len(t, asimovResult, 1, "Should find exactly 1 Asimov")
			assert.Len(t, asimovResult[0].Books, 3,
				"Asimov should have 3 books via reverse edge")

			// Query books by year using predicate name, verify author forward edge
			var booksFrom1954 []PredicateBook
			err = client.Query(ctx, PredicateBook{}).
				Filter(`eq(book_year, 1954)`).
				Nodes(&booksFrom1954)
			require.NoError(t, err, "Query books by year should succeed")
			require.Len(t, booksFrom1954, 2, "Should find 2 books from 1954")

			authorNames := make(map[string]bool)
			for _, b := range booksFrom1954 {
				require.NotNil(t, b.Author,
					"Book %q should have Author populated via forward edge", b.Title)
				authorNames[b.Author.Name] = true
			}
			assert.True(t, authorNames["Tolkien"],
				"Books from 1954 should include one by Tolkien")
			assert.True(t, authorNames["Asimov"],
				"Books from 1954 should include one by Asimov")
		})
	}
}
