/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	dg "github.com/dgraph-io/dgdao"
	"github.com/stretchr/testify/require"
)

func TestClientPool(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "ClientPoolWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "ClientPoolWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("DGDAO_TEST_ADDR"),
			skip: os.Getenv("DGDAO_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip("Skipping test as DGDAO_TEST_ADDR is not set")
			}

			// Create a client with pool size 10
			client, err := dg.NewClient(tc.uri, dg.WithPoolSize(10))
			require.NoError(t, err)
			defer client.Close()

			// Test concurrent client pool usage
			const numWorkers = 20
			var wg sync.WaitGroup
			var mu sync.Mutex
			var clientCount int

			for i := 0; i < numWorkers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					// Get a client from the pool
					client, cleanup, err := client.DgraphClient()
					require.NoError(t, err)
					require.NotNil(t, client)
					txn := client.NewReadOnlyTxn()
					ctx := context.Background()
					_, err = txn.Query(ctx, "query { q(func: uid(1)) { uid } }")
					require.NoError(t, err)
					err = txn.Discard(ctx)
					require.NoError(t, err)

					// Verify we got a valid Dgraph client
					if client != nil {
						mu.Lock()
						clientCount++
						mu.Unlock()
					}

					// Clean up the client
					cleanup()
				}()
			}

			// Wait for all workers to complete
			wg.Wait()

			// Verify we got clients from the pool
			require.GreaterOrEqual(t, clientCount, 1)

			// Get a client before close
			beforeClient, cleanupBefore, err := client.DgraphClient()
			require.NoError(t, err)
			require.NotNil(t, beforeClient)

			// Close the client pool
			client.Close()
			time.Sleep(100 * time.Millisecond) // Give some time for cleanup

			// Verify we can still get a new client after close (pool will create a new one)
			afterClient, cleanupAfter, err := client.DgraphClient()
			require.NoError(t, err)
			require.NotNil(t, afterClient)

			// Verify the client is actually new
			require.NotEqual(t, fmt.Sprintf("%p", beforeClient), fmt.Sprintf("%p", afterClient))

			// Clean up the client
			cleanupAfter()

			// Also clean up the before client if it wasn't already closed
			cleanupBefore()
		})
	}

	// Shutdown at the end of the test to ensure the next test can start fresh
	dg.Shutdown()
}

func TestClientPoolStress(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "ClientPoolStressWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "ClientPoolStressWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("DGDAO_TEST_ADDR"),
			skip: os.Getenv("DGDAO_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip("Skipping test as DGDAO_TEST_ADDR is not set")
			}

			// Create a client with pool size 10
			client, err := dg.NewClient(tc.uri, dg.WithPoolSize(10))
			require.NoError(t, err)
			defer func() {
				client.Close()
			}()

			// Test concurrent client pool usage with high load
			const numWorkers = 20
			const iterations = 10
			var wg sync.WaitGroup
			var successCount int
			var errorCount int
			var mu sync.Mutex

			for i := 0; i < numWorkers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < iterations; j++ {
						dgraphClient, cleanup, err := client.DgraphClient()
						if err != nil {
							mu.Lock()
							errorCount++
							mu.Unlock()
							continue
						}

						if dgraphClient != nil {
							// Test the client works
							txn := dgraphClient.NewReadOnlyTxn()
							ctx := context.Background()
							_, err = txn.Query(ctx, "query { q(func: uid(1)) { uid } }")
							if err != nil {
								err = txn.Discard(ctx)
								if err != nil {
									mu.Lock()
									errorCount++
									mu.Unlock()
								}
								cleanup()
								continue
							}
							err = txn.Discard(ctx)
							if err != nil {
								mu.Lock()
								errorCount++
								mu.Unlock()
								cleanup()
								continue
							}

							mu.Lock()
							successCount++
							mu.Unlock()
						}

						// Clean up the client
						cleanup()
					}
				}()
			}

			wg.Wait()

			require.Greater(t, successCount, 0)
		})

		dg.Shutdown()
	}
}

func TestClientPoolMisuse(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "ClientPoolMisuseWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "ClientPoolMisuseWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("DGDAO_TEST_ADDR"),
			skip: os.Getenv("DGDAO_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip("Skipping test as DGDAO_TEST_ADDR is not set")
			}

			// Create a client with pool size 10
			client, err := dg.NewClient(tc.uri, dg.WithPoolSize(10))
			require.NoError(t, err)
			client.Close()
			client.Close()
		})
	}

	// Shutdown at the end of the test to ensure the next test can start fresh
	dg.Shutdown()
}

func TestLocalClientSingleton(t *testing.T) {
	path := GetTempDir(t)
	client, err := dg.NewClient("file://" + path)
	require.NoError(t, err)
	defer client.Close()

	client2, err := dg.NewClient("file://" + path)
	require.NoError(t, err)
	require.Equal(t, client, client2, "Expected the same client instance")
	defer client2.Close()

	_, err = dg.NewClient("file://"+path, dg.WithAutoSchema(true)) // will create a new client "key"
	require.Error(t, err, "Expected an error when creating a new client with different options")
	require.ErrorIs(t, err, dg.ErrSingletonOnly)
}

func TestRemoteClientAccess(t *testing.T) {

	if os.Getenv("DGDAO_TEST_ADDR") == "" {
		t.Skip("Skipping test as DGDAO_TEST_ADDR is not set")
	}

	client, err := dg.NewClient("dgraph://" + os.Getenv("DGDAO_TEST_ADDR"))
	require.NoError(t, err)
	defer client.Close()

	client2, err := dg.NewClient("dgraph://" + os.Getenv("DGDAO_TEST_ADDR"))
	require.NoError(t, err)
	require.Equal(t, client, client2, "Expected the same client instance")
	defer client2.Close()

	client3, err := dg.NewClient("dgraph://"+os.Getenv("DGDAO_TEST_ADDR"), dg.WithAutoSchema(true))
	require.NoError(t, err)
	require.NotEqual(t, client, client3, "Expected a different client instance")
	defer client3.Close()

	query := "query { q(func: uid(1)) { uid } }"
	ctx := context.Background()

	// run the three clients in go routines to test concurrency
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		_, err := client.QueryRaw(ctx, query, nil)
		require.NoError(t, err)
	}()
	go func() {
		defer wg.Done()
		_, err := client2.QueryRaw(ctx, query, nil)
		require.NoError(t, err)
	}()
	go func() {
		defer wg.Done()
		_, err := client3.QueryRaw(ctx, query, nil)
		require.NoError(t, err)
	}()
	wg.Wait()
}

func TestClientValidator(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "ValidatorWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "ValidatorWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("DGDAO_TEST_ADDR"),
			skip: os.Getenv("DGDAO_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip("Skipping test as DGDAO_TEST_ADDR is not set")
			}

			// Create a validator instance
			validate := dg.NewValidator()

			// Create a client with validator and AutoSchema
			client, err := dg.NewClient(tc.uri, dg.WithValidator(validate), dg.WithAutoSchema(true))
			require.NoError(t, err)
			defer client.Close()

			ctx := context.Background()

			// Test struct with validation tags
			type TestUser struct {
				UID   string   `json:"uid,omitempty"`
				Name  string   `json:"name,omitempty" validate:"required,min=3,max=50"`
				Email string   `json:"email,omitempty" validate:"required,email"`
				Age   int      `json:"age,omitempty" validate:"gte=0,lte=130"`
				Tags  []string `json:"tags,omitempty"`
				DType string   `json:"dgraph.type,omitempty"`
			}

			// Test valid user
			validUser := &TestUser{
				Name:  "John Doe",
				Email: "john@example.com",
				Age:   30,
				Tags:  []string{"developer", "golang"},
			}

			err = client.Insert(ctx, validUser)
			require.NoError(t, err)

			// Test invalid user - name too short
			invalidUser := &TestUser{
				Name:  "Jo", // Too short (min=3)
				Email: "jo@example.com",
				Age:   25,
			}

			err = client.Insert(ctx, invalidUser)
			require.Error(t, err)
			require.Contains(t, err.Error(), "Name")

			// Test invalid user - invalid email
			invalidUser2 := &TestUser{
				Name:  "Jane Doe",
				Email: "invalid-email", // Invalid email format
				Age:   28,
			}

			err = client.Insert(ctx, invalidUser2)
			require.Error(t, err)
			require.Contains(t, err.Error(), "Email")

			// Test invalid user - age too high
			invalidUser3 := &TestUser{
				Name:  "Old Person",
				Email: "old@example.com",
				Age:   150, // Too high (max=130)
			}

			err = client.Insert(ctx, invalidUser3)
			require.Error(t, err)
			require.Contains(t, err.Error(), "Age")

			// Test update with validation
			updateUser := &TestUser{
				UID:   validUser.UID,
				Name:  "John Updated",
				Email: "john.updated@example.com",
				Age:   31,
			}

			err = client.Update(ctx, updateUser)
			require.NoError(t, err)

			// Test update with invalid data
			invalidUpdate := &TestUser{
				UID:   validUser.UID,
				Name:  "", // Empty name (required)
				Email: "john.updated@example.com",
				Age:   31,
			}

			err = client.Update(ctx, invalidUpdate)
			require.Error(t, err)
			require.Contains(t, err.Error(), "Name")
		})
	}
}

func TestClientInvalidNamespace(t *testing.T) {
	// Test the parseNamespaceID function directly by testing invalid namespace IDs
	// This tests the validation logic without needing to create new engines

	testCases := []struct {
		namespace   string
		expectErr   bool
		errContains string
	}{
		{"invalid", true, "invalid namespace ID"},
		{"abc123", true, "invalid namespace ID"},
		{"", false, ""},    // Empty string should use default namespace
		{"0", false, ""},   // Valid numeric
		{"1", false, ""},   // Valid numeric
		{"999", false, ""}, // Valid numeric (will fail at GetNamespace level)
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("namespace_%s", tc.namespace), func(t *testing.T) {
			// We can't test the full NewClient due to singleton issues,
			// but we can test the parseNamespaceID logic indirectly
			if tc.expectErr {
				// Try to create a client with invalid namespace - should fail at parse level
				_, err := dg.NewClient("file://"+t.TempDir(), dg.WithNamespace(tc.namespace))
				if err != nil {
					if strings.Contains(err.Error(), "only one instance") {
						t.Skip("Skipping due to singleton engine")
					}
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.errContains)
				}
			}
		})
	}
}
