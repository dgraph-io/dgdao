/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dolan-in/dgman/v2"
)

// SimString is a string type that participates in automatic vector similarity search.
// When a struct field of this type is tagged with `dgraph:"embedding"`, dgdao
// will automatically generate and maintain a shadow float32vector predicate
// (<fieldname>__vec) backed by the configured EmbeddingProvider.
//
// Example:
//
//	type Product struct {
//	    Description SimString `json:"description,omitempty" dgraph:"embedding,index=term"`
//	    UID         string    `json:"uid,omitempty"`
//	    DType       []string  `json:"dgraph.type,omitempty"`
//	}
type SimString string

// MarshalJSON implements json.Marshaler. SimString serializes as a plain JSON string.
func (s SimString) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(s))
}

// UnmarshalJSON implements json.Unmarshaler. SimString deserializes from a plain JSON string.
func (s *SimString) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	*s = SimString(str)
	return nil
}

// SchemaType implements the dgman SchemaType interface so that dgman emits
// "string" as the Dgraph predicate type for SimString fields.
func (s SimString) SchemaType() string {
	return "string"
}

// EmbeddingProvider is the interface for generating float32 vector embeddings from text.
// Implement this interface to integrate any embedding service (OpenAI, Ollama, local models, etc.).
type EmbeddingProvider interface {
	// Embed returns a float32 embedding vector for the given text.
	Embed(ctx context.Context, text string) ([]float32, error)

	// Dims returns the fixed number of dimensions produced by this provider.
	Dims() int
}

// OpenAICompatibleConfig configures an OpenAI-compatible embedding provider.
// This works with both OpenAI and Ollama (which exposes /v1/embeddings since v0.13.3).
type OpenAICompatibleConfig struct {
	// BaseURL is the base URL of the embedding service.
	// For OpenAI: "https://api.openai.com"
	// For Ollama: "http://localhost:11434"
	BaseURL string

	// Model is the embedding model to use.
	// For OpenAI: e.g. "text-embedding-3-small"
	// For Ollama: e.g. "nomic-embed-text"
	Model string

	// APIKey is the API key for authentication. Leave empty for Ollama.
	APIKey string

	// Dims is the expected number of dimensions for embeddings from this model.
	// This must match the model's actual output dimension.
	Dims int
}

// OpenAICompatibleProvider is an EmbeddingProvider that calls any OpenAI-compatible
// /v1/embeddings endpoint. Works with OpenAI, Ollama, and other compatible services.
type OpenAICompatibleProvider struct {
	config     OpenAICompatibleConfig
	httpClient *http.Client
}

// NewOpenAICompatibleProvider creates a new OpenAICompatibleProvider with the given config.
func NewOpenAICompatibleProvider(config OpenAICompatibleConfig) *OpenAICompatibleProvider {
	return &OpenAICompatibleProvider{
		config:     config,
		httpClient: &http.Client{},
	}
}

type embeddingRequest struct {
	Input string `json:"input"`
	Model string `json:"model"`
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

// Embed implements EmbeddingProvider.
func (p *OpenAICompatibleProvider) Embed(ctx context.Context, text string) ([]float32, error) {
	reqBody := embeddingRequest{
		Input: text,
		Model: p.config.Model,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("embedding: marshal request: %w", err)
	}

	url := strings.TrimRight(p.config.BaseURL, "/") + "/v1/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("embedding: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if p.config.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.config.APIKey)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding: http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("embedding: server returned %d: %s", resp.StatusCode, string(body))
	}

	var embResp embeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&embResp); err != nil {
		return nil, fmt.Errorf("embedding: decode response: %w", err)
	}
	if len(embResp.Data) == 0 {
		return nil, fmt.Errorf("embedding: empty response data")
	}
	return embResp.Data[0].Embedding, nil
}

// Dims implements EmbeddingProvider.
func (p *OpenAICompatibleProvider) Dims() int {
	return p.config.Dims
}

// simFieldInfo holds metadata about a detected SimString field.
type simFieldInfo struct {
	jsonPredicate string // e.g. "description"
	vecPredicate  string // e.g. "description__vec"
	metric        string // default "cosine"
	exponent      string // default "4"
	threshold     int    // min rune count to embed; 0 = always embed
}

// vecShadowPredicate returns the shadow vector predicate name for a given field predicate.
func vecShadowPredicate(predicate string) string {
	return predicate + "__vec"
}

// parseEmbeddingTag parses embedding-specific options from a dgraph struct tag.
// It extracts metric, exponent, and threshold values, returning defaults if not set.
func parseEmbeddingTag(tag string) (metric, exponent string, threshold int) {
	metric = "cosine"
	exponent = "4"
	for _, part := range strings.Split(tag, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "metric=") {
			metric = strings.TrimPrefix(part, "metric=")
		} else if strings.HasPrefix(part, "exponent=") {
			exponent = strings.TrimPrefix(part, "exponent=")
		} else if strings.HasPrefix(part, "threshold=") {
			if n, err := strconv.Atoi(strings.TrimPrefix(part, "threshold=")); err == nil && n >= 0 {
				threshold = n
			}
		}
	}
	return metric, exponent, threshold
}

// hasEmbeddingTag reports whether a dgraph struct tag contains the "embedding" directive.
func hasEmbeddingTag(tag string) bool {
	for _, part := range strings.Split(tag, ",") {
		if strings.TrimSpace(part) == "embedding" {
			return true
		}
	}
	return false
}

// hasSimStringFields reports whether obj contains any SimString field tagged dgraph:"embedding".
// Used as a fast check before allocating a two-phase transaction.
func hasSimStringFields(obj any) bool {
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	simStringType := reflect.TypeOf(SimString(""))

	checkStruct := func(sv reflect.Value) bool {
		if sv.Kind() != reflect.Struct {
			return false
		}
		st := sv.Type()
		for i := 0; i < st.NumField(); i++ {
			field := st.Field(i)
			if field.Type == simStringType && hasEmbeddingTag(field.Tag.Get("dgraph")) {
				return true
			}
		}
		return false
	}

	switch val.Kind() {
	case reflect.Slice:
		for i := 0; i < val.Len(); i++ {
			elem := val.Index(i)
			if elem.Kind() == reflect.Ptr {
				elem = elem.Elem()
			}
			if checkStruct(elem) {
				return true
			}
		}
	case reflect.Struct:
		return checkStruct(val)
	}
	return false
}

// collectSimFieldInfoFromType inspects a struct type (not values) and returns
// metadata about all SimString fields tagged dgraph:"embedding".
// Used by UpdateSchema to emit shadow vector predicates without needing actual data.
func collectSimFieldInfoFromType(t reflect.Type) []simFieldInfo {
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	simStringType := reflect.TypeOf(SimString(""))
	var results []simFieldInfo

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Type != simStringType {
			continue
		}
		dgraphTag := field.Tag.Get("dgraph")
		if !hasEmbeddingTag(dgraphTag) {
			continue
		}
		jsonTag := field.Tag.Get("json")
		predicate := strings.Split(jsonTag, ",")[0]
		if predicate == "" {
			predicate = field.Name
		}
		metric, exponent, threshold := parseEmbeddingTag(dgraphTag)
		results = append(results, simFieldInfo{
			jsonPredicate: predicate,
			vecPredicate:  vecShadowPredicate(predicate),
			metric:        metric,
			exponent:      exponent,
			threshold:     threshold,
		})
	}
	return results
}

// collectSimFields inspects obj (pointer to struct, or slice of pointer to struct)
// and returns metadata about all SimString fields tagged dgraph:"embedding",
// including the current text value of each field.
func collectSimFields(obj any) []simFieldInfo {
	return collectSimFieldInfoFromType(reflect.TypeOf(obj))
}

// buildVecSchemaStatement produces a Dgraph schema line for a shadow vector predicate.
func buildVecSchemaStatement(info simFieldInfo) string {
	return fmt.Sprintf(`%s: float32vector @index(hnsw(exponent: "%s", metric: "%s")) .`,
		info.vecPredicate, info.exponent, info.metric)
}

// vectorToQueryString converts a []float32 to the string format used in Dgraph
// similar_to query variables: "[v1, v2, ...]"
func vectorToQueryString(vec []float32) string {
	parts := make([]string, len(vec))
	for i, v := range vec {
		parts[i] = fmt.Sprintf("%v", v)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// vectorToBytes converts a []float32 to little-endian binary bytes suitable
// for the api.Value_Vfloat32Val field in Dgraph NQuad mutations.
func vectorToBytes(vec []float32) []byte {
	buf := new(bytes.Buffer)
	for _, v := range vec {
		// binary.Write with float32 is not directly supported; use uint32 bit representation
		_ = binary.Write(buf, binary.LittleEndian, v)
	}
	return buf.Bytes()
}

// injectShadowVectors calls the embedding provider for any SimString fields in obj,
// then writes the resulting vectors as NQuad mutations against the already-assigned UIDs.
// It uses the raw dgo.Txn to issue a Set mutation without CommitNow so the
// caller can commit the whole transaction atomically.
func injectShadowVectors(ctx context.Context,
	provider EmbeddingProvider,
	tx *dgman.TxnContext,
	obj any,
	uids []string) error {
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	var structs []reflect.Value
	switch val.Kind() {
	case reflect.Slice:
		for i := 0; i < val.Len(); i++ {
			elem := val.Index(i)
			if elem.Kind() == reflect.Ptr {
				elem = elem.Elem()
			}
			if elem.Kind() == reflect.Struct {
				structs = append(structs, elem)
			}
		}
	case reflect.Struct:
		structs = append(structs, val)
	}

	if len(structs) == 0 {
		return nil
	}

	simStringType := reflect.TypeOf(SimString(""))
	var setNquads []*api.NQuad
	var delNquads []*api.NQuad

	for _, sv := range structs {
		// Always read UID from the struct field — dgman's setUIDHook writes it back
		// after mutation, and the returned uids slice has non-deterministic order
		// (it comes from a map[string]string iteration).
		uidField := sv.FieldByName("UID")
		uid := ""
		if uidField.IsValid() && uidField.Kind() == reflect.String {
			uid = uidField.String()
		}
		if uid == "" {
			continue
		}

		st := sv.Type()
		for i := 0; i < st.NumField(); i++ {
			field := st.Field(i)
			if field.Type != simStringType {
				continue
			}
			dgraphTag := field.Tag.Get("dgraph")
			if !hasEmbeddingTag(dgraphTag) {
				continue
			}

			jsonTag := field.Tag.Get("json")
			jsonParts := strings.Split(jsonTag, ",")
			predicate := jsonParts[0]
			if predicate == "" {
				predicate = field.Name
			}
			hasOmitempty := false
			for _, opt := range jsonParts[1:] {
				if opt == "omitempty" {
					hasOmitempty = true
					break
				}
			}
			vecPred := vecShadowPredicate(predicate)

			textVal := string(sv.Field(i).Interface().(SimString))
			_, _, threshold := parseEmbeddingTag(dgraphTag)

			// Empty + omitempty means the caller didn't set this field on a partial
			// update. The primary predicate is omitted from the JSON mutation (so
			// Dgraph never sees `fn_doc` and the existing string is preserved); the
			// shadow vector must follow the same lifecycle, otherwise partial Updates
			// (e.g. setting just an edge predicate) silently wipe vectors written by
			// a prior Insert/Upsert. To explicitly clear a SimString, drop omitempty
			// so the empty value reaches Dgraph and we delete the shadow vec below.
			if textVal == "" && hasOmitempty {
				continue
			}

			// Below threshold (or explicitly cleared): delete the shadow vector to avoid stale data.
			if textVal == "" || (threshold > 0 && len([]rune(textVal)) < threshold) {
				delNquads = append(delNquads, &api.NQuad{
					Subject:     uid,
					Predicate:   vecPred,
					ObjectValue: &api.Value{Val: &api.Value_DefaultVal{DefaultVal: "_STAR_ALL"}},
				})
				continue
			}

			vec, err := provider.Embed(ctx, textVal)
			if err != nil {
				return fmt.Errorf("embedding field %q: %w", predicate, err)
			}

			setNquads = append(setNquads, &api.NQuad{
				Subject:   uid,
				Predicate: vecPred,
				ObjectValue: &api.Value{
					Val: &api.Value_Vfloat32Val{
						Vfloat32Val: vectorToBytes(vec),
					},
				},
			})
		}
	}

	if len(setNquads) == 0 && len(delNquads) == 0 {
		return nil
	}

	_, err := tx.Txn().Mutate(ctx, &api.Mutation{
		Set:       setNquads,
		Del:       delNquads,
		CommitNow: false,
	})
	return err
}

// SimilarTo returns a QueryBlock ready to Scan() for the k nearest neighbours
// of the given pre-computed vector. It uses $vec as a query variable so Dgraph
// can parse the query with the standard variable substitution path.
//
// The QueryBlock already has the vector variable bound; call Scan() directly:
//
//	vec := []float32{0.1, 0.2, 0.3}
//	dgoClient, cleanup, _ := client.DgraphClient(); defer cleanup()
//	tx := dgman.NewReadOnlyTxn(dgoClient)
//	err := SimilarTo(tx, &result, "description", vec, 5).Scan()
func SimilarTo(tx *dgman.TxnContext, model any, field string, vec []float32, k int) *dgman.QueryBlock {
	vecStr := vectorToQueryString(vec)
	rootFunc := fmt.Sprintf("similar_to(%s, %d, $vec)", vecShadowPredicate(field), k)
	q := dgman.NewQuery().Model(model).RootFunc(rootFunc)
	return tx.Query(q).Vars("similar_to($vec: string)", map[string]string{"$vec": vecStr})
}

// SimilarToText embeds text on-the-fly using the client's configured EmbeddingProvider,
// executes a similar_to(<field>__vec, k, $vec) query, and scans the nearest-neighbour
// results into model. The connection lifecycle is managed internally.
//
// Returns an error if no EmbeddingProvider is configured on the client, embedding fails,
// or the query fails.
//
// Example:
//
//	err := SimilarToText(client, ctx, &result, "description", "fast red sports car", 5)
//	if err != nil { ... }
func SimilarToText(c Client, ctx context.Context, model any, field string, text string, k int) error {
	ec, ok := c.(embeddingClient)
	if !ok {
		return fmt.Errorf("client does not expose embeddingProvider; ensure it is a dgdao client")
	}
	provider := ec.embeddingProvider()
	if provider == nil {
		return fmt.Errorf("no EmbeddingProvider configured on client; use WithEmbeddingProvider")
	}

	vec, err := provider.Embed(ctx, text)
	if err != nil {
		return fmt.Errorf("SimilarToText: embed text: %w", err)
	}

	vecStr := vectorToQueryString(vec)
	rootFunc := fmt.Sprintf("similar_to(%s, %d, $vec)", vecShadowPredicate(field), k)

	dgoClient, cleanup, err := c.DgraphClient()
	if err != nil {
		return err
	}
	defer cleanup()

	tx := dgman.NewReadOnlyTxn(dgoClient)
	q := dgman.NewQuery().Model(model).RootFunc(rootFunc)
	return tx.Query(q).Vars("similar_to($vec: string)", map[string]string{"$vec": vecStr}).Scan()
}

// embeddingClient is an internal interface implemented by client to expose
// the embedding provider to top-level helper functions.
type embeddingClient interface {
	embeddingProvider() EmbeddingProvider
}
