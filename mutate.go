/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/dolan-in/dgman/v2"
)

// parseNamespaceID parses a namespace string to uint64
func parseNamespaceID(ns string) (uint64, error) {
	return strconv.ParseUint(ns, 10, 64)
}

// checkObject validates the passed obj. If it's a slice or a pointer
// to a slice, it returns the first element of the slice. Ultimately,
// the object discovered must be pointer.
func checkObject(obj any) (any, error) {
	val := reflect.ValueOf(obj)

	validateSlice := func(val reflect.Value) (interface{}, error) {
		if val.Len() == 0 {
			return nil, errors.New("slice cannot be empty")
		}

		firstElem := val.Index(0)
		if firstElem.Kind() != reflect.Ptr {
			return nil, errors.New("slice elements must be pointers")
		}

		return firstElem.Interface(), nil
	}

	if val.Kind() == reflect.Ptr && val.Elem().Kind() == reflect.Slice {
		return validateSlice(val.Elem())
	}

	if val.Kind() == reflect.Slice {
		return validateSlice(val)
	}

	if val.Kind() != reflect.Ptr {
		return obj, errors.New("object must be a pointer")
	}
	return obj, nil
}

func (c client) process(ctx context.Context,
	obj any, operation string,
	txFunc func(*dgman.TxnContext, any) ([]string, error)) error {

	schemaObj, err := checkObject(obj)
	if err != nil {
		return err
	}
	if c.options.autoSchema {
		// A type's generated schema can't change within a process, so once
		// UpdateSchema has succeeded for it there is nothing left to sync.
		goType := reflect.TypeOf(schemaObj)
		if !c.options.schemaCache || !c.schemaCache.isSynced(goType) {
			err := c.UpdateSchema(ctx, schemaObj)
			if err != nil {
				return err
			}
			c.schemaCache.markSynced(goType)
		}
	} else {
		// Resolve the Dgraph type name the same way mutations and schema
		// generation do (the DType tag, falling back to the Go struct name).
		// Using the raw Go struct name here would reject types whose Dgraph
		// name differs from the struct name, e.g. a `migrationLock` struct
		// declared as `dgraph:"MigrationLock"`.
		typeName := getNodeType(schemaObj)

		// When AutoSchema is disabled, check schema consistency. Schema
		// alters are additive, so once a type name has been seen in the
		// schema the check can't start failing — skip the fetch when cached.
		if !c.options.schemaCache || !c.schemaCache.isVerified(typeName) {
			currentSchema, err := c.GetSchema(ctx)
			if err != nil {
				return fmt.Errorf("failed to get current schema: %w", err)
			}

			// Validate that required schema exists. Fail if user schema for
			// the type doesn't exist, even if only system schema exists.
			if typeName != "" && !strings.Contains(currentSchema, "type "+typeName) {
				return fmt.Errorf("schema validation failed: database schema does not contain type %s", typeName)
			}
			c.schemaCache.markVerified(typeName)
		}
	}

	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return err
	}
	defer c.pool.put(client)

	provider := c.options.embeddingProvider
	hasEmbedding := provider != nil && hasSimStringFields(obj)

	var tx *dgman.TxnContext
	if hasEmbedding {
		// Do not use SetCommitNow: we need to inject shadow vectors before committing.
		tx = dgman.NewTxnContext(ctx, client)
		// Discard is a no-op after a successful Commit but ensures resources are
		// cleaned up on all paths (error returns, panics, etc.).
		defer func() { _ = tx.Txn().Discard(ctx) }()
	} else {
		tx = dgman.NewTxnContext(ctx, client).SetCommitNow()
	}

	uids, err := txFunc(tx, obj)
	if err != nil {
		// Check if this is a unique constraint violation error from Dgraph
		if uniqueErr := parseUniqueError(err); uniqueErr != nil {
			return uniqueErr
		}
		return err
	}

	if hasEmbedding {
		if err := injectShadowVectors(ctx, provider, tx, obj, uids); err != nil {
			return fmt.Errorf("injecting shadow vectors: %w", err)
		}
		if err := tx.Txn().Commit(ctx); err != nil {
			return fmt.Errorf("committing transaction with shadow vectors: %w", err)
		}
	}

	c.logger.V(2).Info(operation+" successful", "uidCount", len(uids))
	return nil
}

func generateUniquePredicateQuery(predicates map[string]interface{}, nodeType string) (string, map[string]string) {
	var queryBuf bytes.Buffer
	vars := make(map[string]string)

	// Build variable declarations and OR conditions
	varDecls := make([]string, 0, len(predicates))
	conditions := make([]string, 0, len(predicates))
	for key, val := range predicates {
		varType := "string"
		switch val.(type) {
		case int, int32, int64, float32, float64:
			varType = "int"
		}
		varDecls = append(varDecls, fmt.Sprintf("$%s: %s", key, varType))
		conditions = append(conditions, fmt.Sprintf("eq(%s, $%s)", key, key))
		key = fmt.Sprintf("$%s", key)
		vars[key] = fmt.Sprintf("%v", val)
	}
	queryBuf.WriteString("query q(")
	queryBuf.WriteString(strings.Join(varDecls, ", "))
	queryBuf.WriteString(") {\n")
	queryBuf.WriteString(fmt.Sprintf("  q(func: type(%s)) @filter(%s) {\n", nodeType, strings.Join(conditions, " OR ")))
	queryBuf.WriteString("    uid\n  }\n")
	queryBuf.WriteString("}\n")

	return queryBuf.String(), vars
}

func getNodeType(obj any) string {
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	dtypeField := v.FieldByName("DType")
	var nodeType string
	if dtypeField.IsValid() && dtypeField.Kind() == reflect.Slice && dtypeField.Len() > 0 {
		nodeType = dtypeField.Index(0).String()
	} else {
		nodeType = v.Type().Name() // fallback if DType is not present or empty
	}
	return nodeType
}

func getUIDValue(obj any) string {
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v.FieldByName("UID").String()
}

func extractUIDFromDgraphQueryResult(resp []byte) (string, error) {
	var result map[string]interface{}
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", err
	}

	q, ok := result["q"].([]interface{})
	if !ok || len(q) == 0 {
		return "", nil
	}

	firstItem, ok := q[0].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("invalid structure in 'q' array")
	}

	uid, ok := firstItem["uid"].(string)
	if !ok {
		return "", fmt.Errorf("uid not found or not a string")
	}
	return uid, nil
}

func getUpsertPredicates(obj any, firstOnly bool) map[string]any {
	return getPredicatesByTag(obj, "upsert", firstOnly)
}

func getUniquePredicates(obj any) map[string]any {
	return getPredicatesByTag(obj, "unique", false)
}

func getPredicatesByTag(obj any, tagName string, firstOnly bool) map[string]any {
	result := make(map[string]any)
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return result
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("dgraph")
		if tag == "" || !strings.Contains(tag, tagName) {
			continue
		}

		var predName string
		if idx := strings.Index(tag, "predicate="); idx != -1 {
			// Find the first comma or space after predicate=
			endIdx := len(tag)
			commaIdx := strings.Index(tag[idx:], ",")
			spaceIdx := strings.Index(tag[idx:], " ")
			if commaIdx != -1 && (spaceIdx == -1 || commaIdx < spaceIdx) {
				endIdx = idx + commaIdx
			} else if spaceIdx != -1 {
				endIdx = idx + spaceIdx
			}
			predName = tag[idx+len("predicate=") : endIdx]
		} else {
			jsonTag := field.Tag.Get("json")
			if jsonTag != "" && jsonTag != "-" {
				commaIdx := strings.Index(jsonTag, ",")
				if commaIdx != -1 {
					predName = jsonTag[:commaIdx]
				} else {
					predName = jsonTag
				}
			} else {
				predName = field.Name
			}
		}
		result[predName] = v.Field(i).Interface()
		if firstOnly {
			break
		}
	}
	return result
}
