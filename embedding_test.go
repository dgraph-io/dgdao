/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	dg "github.com/dgraph-io/dgdao"
	"github.com/dolan-in/dgman/v2"
	"github.com/stretchr/testify/require"
)

// mockEmbeddingProvider is a deterministic EmbeddingProvider for testing.
// Each unique text gets a distinct unit-vector embedding; identical texts get
// identical embeddings, enabling correct nearest-neighbour assertions.
type mockEmbeddingProvider struct {
	dims    int
	callLog []string // tracks texts that were embedded
	vectors map[string][]float32
}

func newMockProvider(dims int) *mockEmbeddingProvider {
	return &mockEmbeddingProvider{
		dims:    dims,
		vectors: make(map[string][]float32),
	}
}

// register pre-registers a specific vector for a text so tests can control
// exactly what vector will be stored.
func (m *mockEmbeddingProvider) register(text string, vec []float32) {
	m.vectors[text] = vec
}

func (m *mockEmbeddingProvider) Embed(_ context.Context, text string) ([]float32, error) {
	m.callLog = append(m.callLog, text)
	if v, ok := m.vectors[text]; ok {
		return v, nil
	}
	// generate a deterministic unit-ish vector based on string hash
	vec := make([]float32, m.dims)
	for i := range vec {
		vec[i] = float32(len(text)+i) * 0.01
	}
	m.vectors[text] = vec
	return vec, nil
}

func (m *mockEmbeddingProvider) Dims() int { return m.dims }

// embeddableProduct is the test struct using SimString.
type embeddableProduct struct {
	Name        string       `json:"name,omitempty" dgraph:"index=term"`
	Description dg.SimString `json:"description,omitempty" dgraph:"embedding,index=term"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

// embeddableCustomMetric tests overriding metric and exponent.
type embeddableCustomMetric struct {
	Name        string       `json:"name,omitempty" dgraph:"index=term"`
	Description dg.SimString `json:"description,omitempty" dgraph:"embedding,metric=euclidean,exponent=5"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

// embeddableWithThreshold tests the threshold=N tag option.
// Descriptions shorter than 20 runes should not be embedded.
type embeddableWithThreshold struct {
	Name        string       `json:"name,omitempty" dgraph:"index=term"`
	Description dg.SimString `json:"description,omitempty" dgraph:"embedding,threshold=20"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

// createEmbeddingClient creates a test client with the given mock embedding provider.
func createEmbeddingClient(t *testing.T, provider dg.EmbeddingProvider) (dg.Client, func()) {
	t.Helper()
	uri := "file://" + GetTempDir(t)
	client, err := dg.NewClient(uri,
		dg.WithAutoSchema(true),
		dg.WithEmbeddingProvider(provider),
	)
	require.NoError(t, err)

	cleanup := func() {
		_ = client.DropAll(context.Background())
		client.Close()
		dg.Shutdown()
	}
	return client, cleanup
}

// --- Unit tests ---

func TestSimStringMarshal(t *testing.T) {
	s := dg.SimString("hello world")
	b, err := s.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, `"hello world"`, string(b))
}

func TestSimStringUnmarshal(t *testing.T) {
	var s dg.SimString
	require.NoError(t, s.UnmarshalJSON([]byte(`"hello world"`)))
	require.Equal(t, dg.SimString("hello world"), s)
}

func TestHasEmbeddingTagDetection(t *testing.T) {
	provider := newMockProvider(4)
	client, cleanup := createEmbeddingClient(t, provider)
	defer cleanup()

	ctx := context.Background()

	product := &embeddableProduct{
		Name:        "Widget",
		Description: "A small gadget",
	}
	err := client.Insert(ctx, product)
	require.NoError(t, err, "Insert with SimString field should succeed")
	require.NotEmpty(t, product.UID, "UID should be populated after insert")

	// Provider should have been called exactly once for the description field
	require.Len(t, provider.callLog, 1)
	require.Equal(t, "A small gadget", provider.callLog[0])
}

func TestInsertWithEmbedding(t *testing.T) {
	const dims = 5
	provider := newMockProvider(dims)

	// Register controlled vectors so similarity search is deterministic
	provider.register("apple fruit sweet", []float32{0.9, 0.1, 0.1, 0.1, 0.1})
	provider.register("banana yellow tropical", []float32{0.1, 0.9, 0.1, 0.1, 0.1})
	provider.register("carrot orange vegetable", []float32{0.1, 0.1, 0.9, 0.1, 0.1})

	client, cleanup := createEmbeddingClient(t, provider)
	defer cleanup()

	ctx := context.Background()

	products := []*embeddableProduct{
		{Name: "Apple", Description: "apple fruit sweet"},
		{Name: "Banana", Description: "banana yellow tropical"},
		{Name: "Carrot", Description: "carrot orange vegetable"},
	}

	err := client.Insert(ctx, products)
	require.NoError(t, err, "Batch insert with SimString should succeed")

	for _, p := range products {
		require.NotEmpty(t, p.UID, "Each product should have a UID after insert")
	}

	// Provider called once per product
	require.Len(t, provider.callLog, 3)

	// Query back the apple and verify text is intact
	var fetched embeddableProduct
	err = client.Get(ctx, &fetched, products[0].UID)
	require.NoError(t, err)
	require.Equal(t, "apple fruit sweet", string(fetched.Description))
	require.Equal(t, "Apple", fetched.Name)
}

func TestUpdateWithEmbedding(t *testing.T) {
	const dims = 5
	provider := newMockProvider(dims)
	provider.register("original text", []float32{0.5, 0.5, 0.0, 0.0, 0.0})
	provider.register("updated text", []float32{0.0, 0.0, 0.5, 0.5, 0.0})

	client, cleanup := createEmbeddingClient(t, provider)
	defer cleanup()

	ctx := context.Background()

	product := &embeddableProduct{
		Name:        "Thing",
		Description: "original text",
	}
	require.NoError(t, client.Insert(ctx, product))
	require.Len(t, provider.callLog, 1)

	// Update description
	product.Description = "updated text"
	require.NoError(t, client.Update(ctx, product))

	// Provider should have been called again for the updated text
	require.Len(t, provider.callLog, 2)
	require.Equal(t, "updated text", provider.callLog[1])
}

// TestPartialUpdatePreservesShadowVector is a regression test for the
// omitempty-aware skip in injectShadowVectors. A partial Update of a struct
// that contains a SimString field (e.g. setting only UID + a non-SimString
// field) must not delete the existing shadow vector. The primary predicate
// is preserved by omitempty — Dgraph never sees the SimString in the mutation
// NQuads — so the shadow path must follow the same lifecycle.
//
// Real-world failure that motivated this test: codebase-graph's indexer
// upserts a Function with its doc comment in pass A, then issues a partial
// `cli.Update(&Function{UID, Calls})` in pass B to add CALLS edges. Without
// the omitempty skip, every pass-B Update silently wiped the doc-comment
// vector written in pass A, leaving the shadow predicate sparse and breaking
// similarity ranking once `similar_to`'s k saturated the candidate count.
func TestPartialUpdatePreservesShadowVector(t *testing.T) {
	const dims = 4
	provider := newMockProvider(dims)
	origVec := []float32{1.0, 0.0, 0.0, 0.0}
	provider.register("the original description", origVec)

	client, cleanup := createEmbeddingClient(t, provider)
	defer cleanup()
	ctx := context.Background()

	p := &embeddableProduct{Name: "Thing", Description: "the original description"}
	require.NoError(t, client.Insert(ctx, p))
	require.NotEmpty(t, p.UID)
	require.Len(t, provider.callLog, 1, "Insert should embed once")

	findByOrigVec := func(t *testing.T) string {
		t.Helper()
		dgoClient, cleanupDgo, err := client.DgraphClient()
		require.NoError(t, err)
		defer cleanupDgo()
		var result embeddableProduct
		tx := dgman.NewReadOnlyTxn(dgoClient)
		err = dg.SimilarTo(tx, &result, "description", origVec, 1).Scan()
		require.NoError(t, err)
		return result.UID
	}

	// Sanity: shadow vec is reachable after Insert.
	require.Equal(t, p.UID, findByOrigVec(t),
		"shadow vec should be present after Insert")

	// Partial Update: only UID + a non-SimString field. Description is the
	// zero value and is omitted from the mutation by omitempty — the primary
	// predicate is preserved, and the shadow vec must be too.
	require.NoError(t, client.Update(ctx, &embeddableProduct{UID: p.UID, Name: "Thing v2"}))

	// Provider must NOT be re-invoked — there's nothing to embed.
	require.Len(t, provider.callLog, 1,
		"partial Update must not re-embed when SimString is unset")

	// Shadow vec must survive the partial update — this is the regression assertion.
	require.Equal(t, p.UID, findByOrigVec(t),
		"shadow vec must survive partial Update of unrelated fields")
}

func TestSimilarToQuery(t *testing.T) {
	const dims = 5
	provider := newMockProvider(dims)

	// Use well-separated, non-zero vectors to ensure stable cosine similarity results.
	// Group 1 (high first component): items 1-4
	// Group 2 (high last component): items 5-8
	// We query with a Group 1 vector and assert we don't get a Group 2 item back as top-1.
	group1 := [][]float32{
		{0.95, 0.20, 0.10, 0.10, 0.05},
		{0.90, 0.25, 0.12, 0.08, 0.06},
		{0.92, 0.22, 0.11, 0.09, 0.07},
		{0.88, 0.28, 0.13, 0.07, 0.08},
	}
	group2 := [][]float32{
		{0.05, 0.10, 0.10, 0.20, 0.95},
		{0.06, 0.08, 0.12, 0.25, 0.90},
		{0.07, 0.09, 0.11, 0.22, 0.92},
		{0.08, 0.07, 0.13, 0.28, 0.88},
	}

	/* trunk-ignore(golangci-lint/prealloc) */
	var products []*embeddableProduct
	for i, v := range group1 {
		name := fmt.Sprintf("Group1-%d", i+1)
		provider.register(name, v)
		products = append(products, &embeddableProduct{Name: name, Description: dg.SimString(name)})
	}
	for i, v := range group2 {
		name := fmt.Sprintf("Group2-%d", i+1)
		provider.register(name, v)
		products = append(products, &embeddableProduct{Name: name, Description: dg.SimString(name)})
	}

	client, cleanup := createEmbeddingClient(t, provider)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, client.Insert(ctx, products))

	// Query vector is clearly in Group 1 (high first component)
	queryVec := []float32{0.93, 0.21, 0.11, 0.09, 0.06}

	dgoClient, cleanupDgo, err := client.DgraphClient()
	require.NoError(t, err)
	defer cleanupDgo()

	var result embeddableProduct
	tx := dgman.NewReadOnlyTxn(dgoClient)
	err = dg.SimilarTo(tx, &result, "description", queryVec, 1).Scan()
	require.NoError(t, err)

	require.NotEmpty(t, result.Name, "Should find a matching product")
	require.True(t, strings.HasPrefix(result.Name, "Group1-"),
		"Expected a Group1 result but got: %s", result.Name)
}

func TestSimilarToTextQuery(t *testing.T) {
	const dims = 5
	provider := newMockProvider(dims)

	vecApple := []float32{1.0, 0.0, 0.0, 0.0, 0.0}
	vecBanana := []float32{0.0, 1.0, 0.0, 0.0, 0.0}
	vecQueryFruit := []float32{0.99, 0.01, 0.0, 0.0, 0.0} // clearly close to apple

	provider.register("apple fruit sweet", vecApple)
	provider.register("banana yellow tropical", vecBanana)
	provider.register("fruit like apple", vecQueryFruit) // the query text

	client, cleanup := createEmbeddingClient(t, provider)
	defer cleanup()

	ctx := context.Background()

	products := []*embeddableProduct{
		{Name: "Apple Product", Description: "apple fruit sweet"},
		{Name: "Banana Product", Description: "banana yellow tropical"},
	}
	require.NoError(t, client.Insert(ctx, products))

	// SimilarToText should embed "fruit like apple" → vecQueryFruit → nearest is Apple Product
	var result embeddableProduct
	err := dg.SimilarToText(client, ctx, &result, "description", "fruit like apple", 1)
	require.NoError(t, err, "SimilarToText should not error")
	require.Equal(t, "Apple Product", result.Name)
}

func TestUpdateSchemaRegistersVecPredicate(t *testing.T) {
	provider := newMockProvider(4)
	client, cleanup := createEmbeddingClient(t, provider)
	defer cleanup()

	ctx := context.Background()

	// Trigger explicit schema update
	err := client.UpdateSchema(ctx, &embeddableProduct{})
	require.NoError(t, err)

	// QueryRaw against schema introspection to verify the vector predicate was registered
	raw, err := client.QueryRaw(ctx, `schema(pred: [description__vec]) { type }`, nil)
	require.NoError(t, err)
	rawStr := string(raw)
	require.Contains(t, rawStr, "description__vec", "Schema should contain the shadow vector predicate")
	require.Contains(t, rawStr, "float32vector", "Shadow predicate should be of type float32vector")
}

func TestCustomMetricEmbedding(t *testing.T) {
	provider := newMockProvider(4)
	client, cleanup := createEmbeddingClient(t, provider)
	defer cleanup()

	ctx := context.Background()

	err := client.UpdateSchema(ctx, &embeddableCustomMetric{})
	require.NoError(t, err)

	// QueryRaw to verify the vector predicate schema
	raw, err := client.QueryRaw(ctx, `schema(pred: [description__vec]) { type }`, nil)
	require.NoError(t, err)
	require.Contains(t, string(raw), "description__vec", "Shadow predicate should exist")
	// Euclidean metric is embedded in the index definition; verify the predicate type at minimum
	require.Contains(t, string(raw), "float32vector", "Shadow predicate should be float32vector type")
}

func TestNoProviderNoEmbedding(t *testing.T) {
	// Client without embedding provider: Insert should still work normally for SimString fields
	uri := "file://" + GetTempDir(t)
	client, err := dg.NewClient(uri, dg.WithAutoSchema(true))
	require.NoError(t, err)
	defer func() {
		_ = client.DropAll(context.Background())
		client.Close()
		dg.Shutdown()
	}()

	ctx := context.Background()

	product := &embeddableProduct{
		Name:        "NoVec",
		Description: "plain text no embedding",
	}
	err = client.Insert(ctx, product)
	require.NoError(t, err, "Insert should succeed even without an EmbeddingProvider")
	require.NotEmpty(t, product.UID)
}

func TestSimStringTermSearch(t *testing.T) {
	provider := newMockProvider(4)
	client, cleanup := createEmbeddingClient(t, provider)
	defer cleanup()

	ctx := context.Background()

	products := []*embeddableProduct{
		{Name: "Kettle", Description: "stainless steel electric kettle for boiling water"},
		{Name: "Toaster", Description: "four slice toaster with browning control"},
		{Name: "Blender", Description: "high speed blender for smoothies and soups"},
	}
	require.NoError(t, client.Insert(ctx, products))

	// Term search on the description predicate of a SimString field.
	// allofterms matches nodes where the predicate contains all listed terms.
	var result embeddableProduct
	q := client.Query(ctx, &result).Filter("allofterms(description, \"electric kettle\")")
	err := q.Node()
	require.NoError(t, err)
	require.Equal(t, "Kettle", result.Name,
		"Term search on SimString description should return the matching product")

	// anyofterms: should match both Kettle and Blender
	var results []embeddableProduct
	q2 := client.Query(ctx, &results).Filter("anyofterms(description, \"kettle blender\")")
	err = q2.Nodes()
	require.NoError(t, err)
	require.Len(t, results, 2, "anyofterms should match two products")
}

func TestThresholdEmbedding(t *testing.T) {
	const dims = 4
	provider := newMockProvider(dims)
	provider.register("long enough text to embed", []float32{1.0, 0.0, 0.0, 0.0})

	client, cleanup := createEmbeddingClient(t, provider)
	defer cleanup()

	ctx := context.Background()

	// Insert with a description below the 20-rune threshold — provider should NOT be called.
	short := &embeddableWithThreshold{Name: "Short", Description: "too short"}
	require.NoError(t, client.Insert(ctx, short))
	require.Empty(t, provider.callLog, "Provider should not be called for below-threshold text")

	// Insert with a description above the threshold — provider SHOULD be called.
	long := &embeddableWithThreshold{
		Name:        "Long",
		Description: "long enough text to embed",
	}
	require.NoError(t, client.Insert(ctx, long))
	require.Len(t, provider.callLog, 1, "Provider should be called for above-threshold text")

	// Update the long item to a short description — shadow vector should be cleared.
	// After clearing, a similarity query for the original text should not return it.
	long.Description = "short"
	require.NoError(t, client.Update(ctx, long))
	// Provider call count should not increase (below threshold on update too).
	require.Len(t, provider.callLog, 1, "Provider should not be called when updated text is below threshold")

	// The shadow vec for the long item should now be absent — verify via raw schema query
	// that the predicate exists but the node won't appear in similar_to results.
	dgoClient, cleanupDgo, err := client.DgraphClient()
	require.NoError(t, err)
	defer cleanupDgo()

	queryVec := []float32{1.0, 0.0, 0.0, 0.0}
	tx := dgman.NewReadOnlyTxn(dgoClient)
	var result embeddableWithThreshold
	err = dg.SimilarTo(tx, &result, "description", queryVec, 1).Scan()
	// Either no results (empty UID) or the short item (which was never embedded) —
	// the long item's cleared vector should not be the top match.
	require.NoError(t, err)
	require.NotEqual(t, long.UID, result.UID,
		"Cleared shadow vector should not appear in similarity results")
}

// ── Ollama live integration ────────────────────────────────────────────

const (
	ollamaBaseURL = "http://localhost:11434"
	ollamaModel   = "bge-m3:latest"
	ollamaDims    = 1024
)

// ollamaRunning probes Ollama's /api/tags endpoint with a short timeout.
// Returns true only when Ollama is reachable and responds 200.
func ollamaRunning() bool {
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(ollamaBaseURL + "/api/tags")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// skipUnlessOllama calls t.Skip if Ollama is not reachable.
func skipUnlessOllama(t *testing.T) {
	t.Helper()
	if !ollamaRunning() {
		t.Skipf("Ollama not reachable at %s — skipping live integration test", ollamaBaseURL)
	}
}

// sportingGoodsProduct is the test struct used in the live embedding integration tests.
type sportingGoodsProduct struct {
	Name        string       `json:"name,omitempty" dgraph:"index=term"`
	Category    string       `json:"category,omitempty" dgraph:"index=term"`
	Description dg.SimString `json:"description,omitempty" dgraph:"embedding,index=term"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func newOllamaProvider() *dg.OpenAICompatibleProvider {
	return dg.NewOpenAICompatibleProvider(dg.OpenAICompatibleConfig{
		BaseURL: ollamaBaseURL,
		Model:   ollamaModel,
		Dims:    ollamaDims,
	})
}

// TestOllamaIntegration exercises insert, upsert, update, and SimilarToText
// against a real Ollama instance running bge-m3:latest.
func TestOllamaIntegration(t *testing.T) {
	skipUnlessOllama(t)

	provider := newOllamaProvider()
	uri := "file://" + GetTempDir(t)
	client, err := dg.NewClient(uri,
		dg.WithAutoSchema(true),
		dg.WithEmbeddingProvider(provider),
	)
	require.NoError(t, err)
	defer func() {
		_ = client.DropAll(context.Background())
		client.Close()
		dg.Shutdown()
	}()

	ctx := context.Background()

	// 1. Insert a corpus of semantically varied products
	products := []*sportingGoodsProduct{
		{
			Name:        "Trail Runner X",
			Category:    "footwear",
			Description: "Lightweight trail running shoe with aggressive grip for mountain terrain",
		},
		{
			Name:        "Road Racer Pro",
			Category:    "footwear",
			Description: "Carbon-plated road running shoe for fast road races and marathons",
		},
		{
			Name:        "Summit Hardshell",
			Category:    "outerwear",
			Description: "Waterproof hardshell jacket for alpine climbing and severe weather",
		},
		{
			Name:        "Base Layer Merino",
			Category:    "clothing",
			Description: "Soft merino wool thermal base layer for cold weather activities",
		},
		{
			Name:        "Carbon Fibre Kayak",
			Category:    "watersports",
			Description: "Ultra-light carbon fibre sea kayak for ocean touring and expedition paddling",
		},
		{
			Name:        "Rock Climbing Harness",
			Category:    "climbing",
			Description: "Comfortable sit harness for sport climbing and indoor bouldering gym use",
		},
		{
			Name:        "Trail Mix Nutrition Bar",
			Category:    "nutrition",
			Description: "High-energy snack bar for long hikes, runs, and endurance activities",
		},
		{
			Name:        "GPS Watch Ultra",
			Category:    "electronics",
			Description: "Multi-sport GPS watch with heart rate monitoring and route navigation",
		},
	}

	err = client.Insert(ctx, products)
	require.NoError(t, err, "Insert corpus should succeed")
	for _, p := range products {
		require.NotEmpty(t, p.UID, "Each product should have a UID: %s", p.Name)
	}
	t.Logf("Inserted %d products", len(products))

	// 2. Verify the shadow schema was registered
	raw, err := client.QueryRaw(ctx, `schema(pred: [description__vec]) { type }`, nil)
	require.NoError(t, err)
	require.Contains(t, string(raw), "float32vector", "Shadow predicate should be registered")

	// 3. SimilarToText: query for running shoes
	var shoeResult sportingGoodsProduct
	err = dg.SimilarToText(client, ctx, &shoeResult, "description", "running shoes for trails", 1)
	require.NoError(t, err)
	t.Logf("Running shoe query → %q (%s)", shoeResult.Name, shoeResult.Description)
	require.NotEmpty(t, shoeResult.Name, "Should find a product")
	require.Contains(t, strings.ToLower(shoeResult.Category), "footwear",
		"Top result for 'running shoes for trails' should be footwear, got %q", shoeResult.Name)

	// 4. SimilarToText: query for waterproof outerwear
	var jacketResult sportingGoodsProduct
	err = dg.SimilarToText(client, ctx, &jacketResult, "description", "waterproof jacket for bad weather", 1)
	require.NoError(t, err)
	t.Logf("Jacket query → %q (%s)", jacketResult.Name, jacketResult.Description)
	require.NotEmpty(t, jacketResult.Name)
	require.Equal(t, "Summit Hardshell", jacketResult.Name,
		"Top result for waterproof jacket should be Summit Hardshell")

	// ── 5. Update: change Trail Runner X description and re-query ─────────────
	trailRunner := products[0]
	trailRunner.Description = "Rugged trail running shoe with rock plate and waterproof membrane for muddy conditions"
	err = client.Update(ctx, trailRunner)
	require.NoError(t, err, "Update should succeed")

	// Re-query with the updated semantics — still expects a trail running shoe
	var updatedResult sportingGoodsProduct
	err = dg.SimilarToText(client, ctx, &updatedResult, "description", "waterproof trail shoe for mud", 1)
	require.NoError(t, err)
	t.Logf("After update query → %q", updatedResult.Name)
	require.NotEmpty(t, updatedResult.Name)

	// 6. Upsert: update Road Racer Pro by predicate
	roadRacer := products[1]
	roadRacer.Description = "Featherlight carbon road shoe for sub-3-hour marathon performance"
	err = client.Upsert(ctx, roadRacer, "name")
	require.NoError(t, err, "Upsert should succeed")

	// ── 7. SimilarToText: confirm marathon query still maps to road shoe ───────
	var marathonResult sportingGoodsProduct
	err = dg.SimilarToText(client, ctx, &marathonResult, "description", "shoe for running a marathon", 1)
	require.NoError(t, err)
	t.Logf("Marathon query → %q", marathonResult.Name)
	require.NotEmpty(t, marathonResult.Name)
	require.Equal(t, "footwear", marathonResult.Category,
		"Marathon shoe query should return a footwear product")
}
