/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/dgraph/v25/x"
	"google.golang.org/grpc"
)

// embeddedDgraphClient implements api.DgraphClient by routing calls to the embedded Engine.
// This allows dgman and dgo to work seamlessly with the embedded Dgraph.
type embeddedDgraphClient struct {
	engine *Engine
	ns     *Namespace
}

// newEmbeddedDgraphClient creates a new embedded client for the given namespace.
func newEmbeddedDgraphClient(engine *Engine, ns *Namespace) *embeddedDgraphClient {
	return &embeddedDgraphClient{
		engine: engine,
		ns:     ns,
	}
}

func (c *embeddedDgraphClient) Login(
	ctx context.Context,
	in *api.LoginRequest,
	opts ...grpc.CallOption,
) (*api.Response, error) {
	// For embedded mode, login is a no-op (no auth required)
	return &api.Response{}, nil
}

func (c *embeddedDgraphClient) Query(
	ctx context.Context,
	in *api.Request,
	opts ...grpc.CallOption,
) (*api.Response, error) {
	// Attach namespace context
	ctx = x.AttachNamespace(ctx, c.ns.ID())

	// For requests with both query and mutations (upsert case)
	if len(in.Mutations) > 0 && in.Query != "" {
		return c.handleUpsert(ctx, in)
	}

	// Simple mutation (no query)
	if len(in.Mutations) > 0 {
		uids, err := c.ns.Mutate(ctx, in.Mutations)
		if err != nil {
			return nil, err
		}
		// Convert uids map to response format
		// dgman expects keys without the "_:" prefix (it strips prefix when looking up)
		uidStrings := make(map[string]string)
		for k, v := range uids {
			key := k
			if strings.HasPrefix(k, "_:") {
				key = k[2:] // strip "_:" prefix
			}
			uidStrings[key] = fmt.Sprintf("0x%x", v)
		}
		return &api.Response{
			Uids: uidStrings,
			Txn:  &api.TxnContext{StartTs: in.StartTs},
		}, nil
	}

	// Query only
	return c.engine.query(ctx, c.ns, in.Query, in.Vars)
}

// handleUpsert handles upsert requests (query + mutations) for embedded mode.
// It executes the query first to resolve variable UIDs, then substitutes
// uid(var) references in mutations before applying them.
func (c *embeddedDgraphClient) handleUpsert(ctx context.Context, in *api.Request) (*api.Response, error) {
	// Step 1: Transform the upsert query to remove variable definitions
	// dgman sends queries like: q_1_0(...) { u_1_0 as uid }
	// We need to convert to: q_1_0(...) { uid } and map results back
	transformedQuery, varMappings := transformUpsertQuery(in.Query)

	// Step 2: Execute the transformed query
	queryResp, err := c.engine.query(ctx, c.ns, transformedQuery, in.Vars)
	if err != nil {
		return nil, fmt.Errorf("upsert query failed: %w", err)
	}

	// Step 3: Parse query results and map to variable names
	varUIDs, err := extractVarUIDsWithMapping(queryResp.Json, varMappings)
	if err != nil {
		return nil, fmt.Errorf("failed to extract var UIDs: %w", err)
	}

	// Step 4: Substitute uid(var) references in mutations
	for _, mu := range in.Mutations {
		substituteUIDVars(mu, varUIDs)
	}

	// Step 5: Apply mutations using embedded path
	uids, err := c.ns.Mutate(ctx, in.Mutations)
	if err != nil {
		return nil, err
	}

	// Convert uids map to response format
	uidStrings := make(map[string]string)
	for k, v := range uids {
		key := k
		if strings.HasPrefix(k, "_:") {
			key = k[2:]
		}
		uidStrings[key] = fmt.Sprintf("0x%x", v)
	}

	return &api.Response{
		Json: queryResp.Json,
		Uids: uidStrings,
		Txn:  &api.TxnContext{StartTs: in.StartTs},
	}, nil
}

func (c *embeddedDgraphClient) Alter(
	ctx context.Context,
	in *api.Operation,
	opts ...grpc.CallOption,
) (*api.Payload, error) {
	if in.DropAll {
		if err := c.engine.DropAll(ctx); err != nil {
			return nil, err
		}
		return &api.Payload{}, nil
	}
	if in.DropOp == api.Operation_DATA {
		if err := c.engine.dropData(ctx, c.ns); err != nil {
			return nil, err
		}
		return &api.Payload{}, nil
	}
	if in.DropAttr != "" {
		if err := c.engine.dropPredicate(ctx, c.ns, in.DropAttr); err != nil {
			return nil, err
		}
		return &api.Payload{}, nil
	}
	if in.Schema != "" {
		if err := c.engine.alterSchema(ctx, c.ns, in.Schema); err != nil {
			return nil, err
		}
	}
	return &api.Payload{}, nil
}

func (c *embeddedDgraphClient) CommitOrAbort(
	ctx context.Context,
	in *api.TxnContext,
	opts ...grpc.CallOption,
) (*api.TxnContext, error) {
	return c.engine.commitOrAbort(ctx, c.ns, in)
}

func (c *embeddedDgraphClient) CheckVersion(
	ctx context.Context,
	in *api.Check,
	opts ...grpc.CallOption,
) (*api.Version, error) {
	return &api.Version{Tag: "embedded"}, nil
}

func (c *embeddedDgraphClient) RunDQL(
	ctx context.Context,
	in *api.RunDQLRequest,
	opts ...grpc.CallOption,
) (*api.Response, error) {
	return c.engine.query(ctx, c.ns, in.DqlQuery, in.Vars)
}

func (c *embeddedDgraphClient) AllocateIDs(
	ctx context.Context,
	in *api.AllocateIDsRequest,
	opts ...grpc.CallOption,
) (*api.AllocateIDsResponse, error) {
	// Not used in embedded mode - UIDs are allocated during mutation
	return &api.AllocateIDsResponse{}, nil
}

func (c *embeddedDgraphClient) UpdateExtSnapshotStreamingState(
	ctx context.Context,
	in *api.UpdateExtSnapshotStreamingStateRequest,
	opts ...grpc.CallOption,
) (*api.UpdateExtSnapshotStreamingStateResponse, error) {
	// Not supported in embedded mode
	return &api.UpdateExtSnapshotStreamingStateResponse{}, nil
}

func (c *embeddedDgraphClient) StreamExtSnapshot(
	ctx context.Context,
	opts ...grpc.CallOption,
) (api.Dgraph_StreamExtSnapshotClient, error) {
	// Not supported in embedded mode
	return nil, nil
}

func (c *embeddedDgraphClient) CreateNamespace(
	ctx context.Context,
	in *api.CreateNamespaceRequest,
	opts ...grpc.CallOption,
) (*api.CreateNamespaceResponse, error) {
	ns, err := c.engine.CreateNamespace()
	if err != nil {
		return nil, err
	}
	return &api.CreateNamespaceResponse{Namespace: ns.ID()}, nil
}

func (c *embeddedDgraphClient) DropNamespace(
	ctx context.Context,
	in *api.DropNamespaceRequest,
	opts ...grpc.CallOption,
) (*api.DropNamespaceResponse, error) {
	// Not implemented yet
	return &api.DropNamespaceResponse{}, nil
}

func (c *embeddedDgraphClient) ListNamespaces(
	ctx context.Context,
	in *api.ListNamespacesRequest,
	opts ...grpc.CallOption,
) (*api.ListNamespacesResponse, error) {
	// Not implemented yet
	return &api.ListNamespacesResponse{}, nil
}

// varAsRegex matches "varname as uid" patterns in query blocks
var varAsRegex = regexp.MustCompile(`(\w+)\s+as\s+uid`)

// transformUpsertQuery transforms a dgman upsert query to remove variable definitions.
// Input:  { q_1_0(...) { u_1_0 as uid } }
// Output: { q_1_0(...) { uid } } and mapping {"q_1_0": "u_1_0"}
func transformUpsertQuery(query string) (string, map[string]string) {
	varMappings := make(map[string]string)

	// Find all "varname as uid" patterns and extract the variable names
	// dgman uses pattern: q_N_M for query blocks, u_N_M for uid variables
	matches := varAsRegex.FindAllStringSubmatch(query, -1)
	for _, match := range matches {
		if len(match) >= 2 {
			varName := match[1] // e.g., "u_1_0"
			// Convert u_N_M to q_N_M for block name mapping
			if strings.HasPrefix(varName, "u_") {
				blockName := "q_" + varName[2:]
				varMappings[blockName] = varName
			}
		}
	}

	// Replace "varname as uid" with just "uid"
	transformedQuery := varAsRegex.ReplaceAllString(query, "uid")

	return transformedQuery, varMappings
}

// extractVarUIDsWithMapping parses query results and maps block names to variable names
func extractVarUIDsWithMapping(jsonData []byte, varMappings map[string]string) (map[string]string, error) {
	if len(jsonData) == 0 {
		return make(map[string]string), nil
	}

	var result map[string][]map[string]interface{}
	if err := json.Unmarshal(jsonData, &result); err != nil {
		return nil, err
	}

	varUIDs := make(map[string]string)
	for blockName, nodes := range result {
		if len(nodes) > 0 {
			if uid, ok := nodes[0]["uid"].(string); ok {
				// Map block name to variable name
				if varName, ok := varMappings[blockName]; ok {
					varUIDs[varName] = uid
				} else {
					varUIDs[blockName] = uid
				}
			}
		}
	}
	return varUIDs, nil
}

// uidVarRegex matches uid(varname) patterns in mutation data
var uidVarRegex = regexp.MustCompile(`uid\(([^)]+)\)`)

// substituteUIDVars replaces uid(var) references in mutations with actual UIDs
func substituteUIDVars(mu *api.Mutation, varUIDs map[string]string) {
	// Handle SetJson
	if len(mu.SetJson) > 0 {
		mu.SetJson = []byte(substituteInString(string(mu.SetJson), varUIDs))
	}
	// Handle DeleteJson
	if len(mu.DeleteJson) > 0 {
		mu.DeleteJson = []byte(substituteInString(string(mu.DeleteJson), varUIDs))
	}
	// Handle Set NQuads
	for _, nq := range mu.Set {
		substituteInNQuad(nq, varUIDs)
	}
	// Handle Del NQuads
	for _, nq := range mu.Del {
		substituteInNQuad(nq, varUIDs)
	}
}

func substituteInString(s string, varUIDs map[string]string) string {
	return uidVarRegex.ReplaceAllStringFunc(s, func(match string) string {
		// Extract variable name from uid(varname)
		varName := match[4 : len(match)-1] // strip "uid(" and ")"
		if uid, ok := varUIDs[varName]; ok {
			return uid
		}
		// If no UID found, convert to blank node for new entity
		return "_:uid(" + varName + ")"
	})
}

func substituteInNQuad(nq *api.NQuad, varUIDs map[string]string) {
	// Substitute in Subject
	if strings.HasPrefix(nq.Subject, "uid(") {
		varName := nq.Subject[4 : len(nq.Subject)-1]
		if uid, ok := varUIDs[varName]; ok {
			nq.Subject = uid
		}
	}
	// Substitute in ObjectId
	if strings.HasPrefix(nq.ObjectId, "uid(") {
		varName := nq.ObjectId[4 : len(nq.ObjectId)-1]
		if uid, ok := varUIDs[varName]; ok {
			nq.ObjectId = uid
		}
	}
}
