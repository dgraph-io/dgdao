/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/dgraph/v25/dql"
	"github.com/dgraph-io/dgraph/v25/edgraph"
	"github.com/dgraph-io/dgraph/v25/hooks"
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/query"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/worker"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/go-logr/logr"
)

var (
	// This ensures that we only have one instance of dgdao in this process.
	singleton atomic.Bool
	// activeEngine tracks the current Engine instance for global access
	activeEngine *Engine

	ErrSingletonOnly    = errors.New("only one instance of dgdao can exist in a process")
	ErrEmptyDataDir     = errors.New("data directory is required")
	ErrClosedEngine     = errors.New("dgdao engine is closed")
	ErrNonExistentDB    = errors.New("namespace does not exist")
	ErrInvalidCacheSize = errors.New("cache size must be zero or positive")
)

// Engine is an instance of dgdao.
// For now, we only support one instance of dgdao per process.
type Engine struct {
	mutex  sync.RWMutex
	isOpen atomic.Bool

	z *zero

	// points to default / 0 / galaxy namespace
	db0 *Namespace

	logger logr.Logger
}

// NewEngine returns a new dgdao instance.
func NewEngine(conf Config) (*Engine, error) {
	// Ensure that we do not create another instance of dgdao in the same process
	if !singleton.CompareAndSwap(false, true) {
		conf.logger.Error(ErrSingletonOnly, "Failed to create engine")
		return nil, ErrSingletonOnly
	}

	conf.logger.V(1).Info("Creating new dgdao engine", "dataDir", conf.dataDir)

	if err := conf.validate(); err != nil {
		conf.logger.Error(err, "Invalid configuration")
		return nil, err
	}

	// setup data directories
	worker.Config.PostingDir = path.Join(conf.dataDir, "p")
	worker.Config.WALDir = path.Join(conf.dataDir, "w")
	worker.Config.TypeFilterUidLimit = 100000
	x.WorkerConfig.TmpDir = path.Join(conf.dataDir, "t")

	// TODO: optimize these and more options
	x.WorkerConfig.Badger = badger.DefaultOptions("").FromSuperFlag(worker.BadgerDefaults)
	x.Config.MaxRetries = 10
	x.Config.Limit = z.NewSuperFlag("max-pending-queries=100000")
	x.Config.LimitNormalizeNode = conf.limitNormalizeNode
	x.Config.LimitQueryEdge = 1000000      // Allow up to 1M edges in upsert queries
	x.Config.LimitMutationsNquad = 1000000 // Allow up to 1M nquads in mutations

	// initialize each package
	edgraph.Init()
	worker.State.InitStorage()
	worker.InitForLite(worker.State.Pstore)
	schema.Init(worker.State.Pstore)
	cacheSizeBytes := conf.cacheSizeMB * 1024 * 1024
	posting.Init(worker.State.Pstore, int64(cacheSizeBytes), false)

	engine := &Engine{
		logger: conf.logger,
	}
	engine.isOpen.Store(true)
	engine.logger.V(1).Info("Initializing engine state")
	if err := engine.reset(); err != nil {
		engine.logger.Error(err, "Failed to reset database")
		return nil, fmt.Errorf("error resetting db: %w", err)
	}

	// Enable embedded mode with Zero hooks to bypass distributed Zero node
	hooks.Enable(&hooks.Config{
		ZeroHooks: hooks.ZeroHooksFns{
			AssignUIDsFn: func(ctx context.Context, num *pb.Num) (*pb.AssignedIds, error) {
				num.Type = pb.Num_UID
				return engine.z.nextUIDs(num)
			},
			AssignTimestampsFn: func(ctx context.Context, num *pb.Num) (*pb.AssignedIds, error) {
				ts, err := engine.z.nextTs()
				if err != nil {
					return nil, err
				}
				return &pb.AssignedIds{StartId: ts, EndId: ts}, nil
			},
			AssignNsIDsFn: func(ctx context.Context, num *pb.Num) (*pb.AssignedIds, error) {
				num.Type = pb.Num_NS_ID
				nsID, err := engine.z.nextNamespace()
				if err != nil {
					return nil, err
				}
				return &pb.AssignedIds{StartId: nsID, EndId: nsID}, nil
			},
			CommitOrAbortFn: func(ctx context.Context, tc *api.TxnContext) (*api.TxnContext, error) {
				if tc.Aborted {
					return &api.TxnContext{StartTs: tc.StartTs, Aborted: true}, nil
				}
				commitTs, err := engine.z.nextTs()
				if err != nil {
					return nil, err
				}
				// Apply the commit to the oracle
				delta := &pb.OracleDelta{
					Txns: []*pb.TxnStatus{{StartTs: tc.StartTs, CommitTs: commitTs}},
				}
				posting.Oracle().ProcessDelta(delta)
				return &api.TxnContext{StartTs: tc.StartTs, CommitTs: commitTs}, nil
			},
			ApplyMutationsFn: func(ctx context.Context, m *pb.Mutations) (*api.TxnContext, error) {
				// Create a proposal with the mutations
				p := &pb.Proposal{Mutations: m}
				err := worker.ApplyMutations(ctx, p)
				return &api.TxnContext{StartTs: m.StartTs}, err
			},
		},
	})

	// Store the engine as the active instance
	activeEngine = engine
	x.UpdateHealthStatus(true)

	engine.db0 = &Namespace{id: 0, engine: engine}

	return engine, nil
}

// Shutdown closes the active Engine instance and resets the singleton state.
func Shutdown() {
	if activeEngine != nil {
		activeEngine.Close()
		activeEngine = nil
	}
	// Reset the singleton state so a new engine can be created if needed
	singleton.Store(false)
}

func (engine *Engine) CreateNamespace() (*Namespace, error) {
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()

	if !engine.isOpen.Load() {
		return nil, ErrClosedEngine
	}

	startTs, err := engine.z.nextTs()
	if err != nil {
		return nil, err
	}
	nsID, err := engine.z.nextNamespace()
	if err != nil {
		return nil, err
	}

	if err := worker.ApplyInitialSchema(nsID, startTs); err != nil {
		return nil, fmt.Errorf("error applying initial schema: %w", err)
	}
	for _, pred := range schema.State().Predicates() {
		worker.InitTablet(pred)
	}

	return &Namespace{id: nsID, engine: engine}, nil
}

func (engine *Engine) GetNamespace(nsID uint64) (*Namespace, error) {
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()

	return engine.getNamespaceWithLock(nsID)
}

func (engine *Engine) getNamespaceWithLock(nsID uint64) (*Namespace, error) {
	if !engine.isOpen.Load() {
		return nil, ErrClosedEngine
	}

	if nsID > engine.z.lastNamespace {
		return nil, ErrNonExistentDB
	}

	// TODO: when delete namespace is implemented, check if the namespace exists

	return &Namespace{id: nsID, engine: engine}, nil
}

func (engine *Engine) GetDefaultNamespace() *Namespace {
	return engine.db0
}

// DropAll drops all the data and schema in the dg engine instance.
func (engine *Engine) DropAll(ctx context.Context) error {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	if !engine.isOpen.Load() {
		return ErrClosedEngine
	}

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId: 1,
		DropOp:  pb.Mutations_ALL,
	}}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}
	if err := engine.reset(); err != nil {
		return fmt.Errorf("error resetting db: %w", err)
	}

	// TODO: insert drop record
	return nil
}

func (engine *Engine) dropData(ctx context.Context, ns *Namespace) error {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	if !engine.isOpen.Load() {
		return ErrClosedEngine
	}

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId:   1,
		DropOp:    pb.Mutations_DATA,
		DropValue: strconv.FormatUint(ns.ID(), 10),
	}}

	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}

	// TODO: insert drop record
	// TODO: should we reset back the timestamp as well?
	return nil
}

// dropPredicate deletes a single predicate (and its data) from the embedded
// engine — the in-process equivalent of a gRPC Alter with DropAttr set.
func (engine *Engine) dropPredicate(ctx context.Context, ns *Namespace, pred string) error {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	if !engine.isOpen.Load() {
		return ErrClosedEngine
	}

	startTs, err := engine.z.nextTs()
	if err != nil {
		return err
	}

	nsAttr := x.NamespaceAttr(ns.ID(), pred)
	return posting.DeletePredicate(ctx, nsAttr, startTs)
}

func (engine *Engine) alterSchema(ctx context.Context, ns *Namespace, sch string) error {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	if !engine.isOpen.Load() {
		return ErrClosedEngine
	}

	sc, err := schema.ParseWithNamespace(sch, ns.ID())
	if err != nil {
		return fmt.Errorf("error parsing schema: %w", err)
	}
	return engine.alterSchemaWithParsed(ctx, sc)
}

func (engine *Engine) alterSchemaWithParsed(ctx context.Context, sc *schema.ParsedSchema) error {
	for _, pred := range sc.Preds {
		worker.InitTablet(pred.Predicate)
	}

	startTs, err := engine.z.nextTs()
	if err != nil {
		return err
	}

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId: 1,
		StartTs: startTs,
		Schema:  sc.Preds,
		Types:   sc.Types,
	}}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}
	return nil
}

func (engine *Engine) query(ctx context.Context,
	ns *Namespace,
	q string,
	vars map[string]string) (*api.Response, error) {
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()

	return engine.queryWithLock(ctx, ns, q, vars)
}

func (engine *Engine) queryWithLock(ctx context.Context,
	ns *Namespace,
	q string,
	vars map[string]string) (*api.Response, error) {
	if !engine.isOpen.Load() {
		return nil, ErrClosedEngine
	}

	engine.logger.V(2).Info("Querying namespace", "namespaceID", ns.ID(), "query", q)
	ctx = x.AttachNamespace(ctx, ns.ID())
	return (&edgraph.Server{}).QueryNoAuth(ctx, &api.Request{
		ReadOnly: true,
		Query:    q,
		StartTs:  engine.z.readTs(),
		Vars:     vars,
	})
}

func (engine *Engine) mutate(ctx context.Context, ns *Namespace, ms []*api.Mutation) (map[string]uint64, error) {
	if len(ms) == 0 {
		return nil, nil
	}

	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	dms := make([]*dql.Mutation, 0, len(ms))
	for _, mu := range ms {
		dm, err := edgraph.ParseMutationObject(ctx, mu)
		if err != nil {
			return nil, fmt.Errorf("error parsing mutation: %w", err)
		}
		dms = append(dms, dm)
	}
	newUids, err := query.ExtractBlankUIDs(ctx, dms)
	if err != nil {
		return nil, err
	}
	if len(newUids) > 0 {
		num := &pb.Num{Val: uint64(len(newUids)), Type: pb.Num_UID}
		res, err := engine.z.nextUIDs(num)
		if err != nil {
			return nil, err
		}

		curId := res.StartId
		for k := range newUids {
			x.AssertTruef(curId != 0 && curId <= res.EndId, "not enough uids generated")
			newUids[k] = curId
			curId++
		}
	}

	return engine.mutateWithDqlMutation(ctx, ns, dms, newUids)
}

func (engine *Engine) mutateWithDqlMutation(ctx context.Context, ns *Namespace, dms []*dql.Mutation,
	newUids map[string]uint64) (map[string]uint64, error) {
	edges, err := query.ToDirectedEdges(dms, newUids)
	if err != nil {
		return nil, fmt.Errorf("error converting to directed edges: %w", err)
	}
	ctx = x.AttachNamespace(ctx, ns.ID())

	if !engine.isOpen.Load() {
		return nil, ErrClosedEngine
	}

	// Check unique constraints before applying mutations
	if err := engine.verifyUniqueConstraints(ctx, ns, edges, newUids); err != nil {
		return nil, err
	}

	startTs, err := engine.z.nextTs()
	if err != nil {
		return nil, err
	}
	commitTs, err := engine.z.nextTs()
	if err != nil {
		return nil, err
	}

	m := &pb.Mutations{
		GroupId: 1,
		StartTs: startTs,
		Edges:   edges,
	}

	m.Edges, err = query.ExpandEdges(ctx, m)
	if err != nil {
		return nil, fmt.Errorf("error expanding edges: %w", err)
	}

	for _, edge := range m.Edges {
		worker.InitTablet(edge.Attr)
	}

	p := &pb.Proposal{Mutations: m, StartTs: startTs}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return nil, err
	}

	return newUids, worker.ApplyCommited(ctx, &pb.OracleDelta{
		Txns: []*pb.TxnStatus{{StartTs: startTs, CommitTs: commitTs}},
	})
}

// verifyUniqueConstraints checks that mutations don't violate @unique constraints
func (engine *Engine) verifyUniqueConstraints(
	ctx context.Context,
	ns *Namespace,
	edges []*pb.DirectedEdge,
	newUids map[string]uint64,
) error {
	namespace := ns.ID()

	// Track values seen within this mutation batch for in-batch duplicate detection
	// Key: "predName:value", Value: subject UID
	seenValues := make(map[string]uint64)

	for _, edge := range edges {
		// Skip delete operations
		if edge.Op == pb.DirectedEdge_DEL {
			continue
		}

		// Get predicate name - edge.Attr may or may not have namespace prefix
		predName := edge.Attr
		if strings.Contains(predName, x.NsSeparator) {
			predName = x.ParseAttr(edge.Attr)
		}

		// Check if predicate has @unique constraint
		predSchema, ok := schema.State().Get(ctx, x.NamespaceAttr(namespace, predName))
		if !ok || !predSchema.Unique {
			continue
		}

		// Get the value being set
		val := edge.Value
		if len(val) == 0 {
			continue
		}

		valStr := string(val)
		subjectUID := edge.Entity

		// Check for in-batch duplicates first
		key := predName + ":" + valStr
		if existingUID, seen := seenValues[key]; seen {
			if existingUID != subjectUID {
				return &UniqueError{
					Field: predName,
					Value: valStr,
					UID:   fmt.Sprintf("0x%x", existingUID),
				}
			}
		}
		seenValues[key] = subjectUID

		// Build query to check for existing value in database
		queryStr := fmt.Sprintf(`{
			check(func: eq(%s, %q)) {
				uid
			}
		}`, predName, valStr)

		resp, err := engine.queryWithLock(ctx, ns, queryStr, nil)
		if err != nil {
			return fmt.Errorf("error checking unique constraint for %s: %w", predName, err)
		}

		// Parse response to check if any existing UIDs found
		existingUID, found := parseUniqueCheckResponse(resp.Json)
		if !found {
			continue
		}

		// If found, check if it's the same UID (update case is allowed)
		if existingUID != subjectUID {
			return &UniqueError{
				Field: predName,
				Value: valStr,
				UID:   fmt.Sprintf("0x%x", existingUID),
			}
		}
	}

	return nil
}

// parseUniqueCheckResponse parses the query response to find existing UID
func parseUniqueCheckResponse(jsonData []byte) (uint64, bool) {
	if len(jsonData) == 0 {
		return 0, false
	}

	var result struct {
		Check []struct {
			UID string `json:"uid"`
		} `json:"check"`
	}

	if err := json.Unmarshal(jsonData, &result); err != nil {
		return 0, false
	}

	if len(result.Check) == 0 {
		return 0, false
	}

	uidStr := result.Check[0].UID
	if uidStr == "" {
		return 0, false
	}

	// Parse UID (format: "0x123")
	if len(uidStr) > 2 && uidStr[:2] == "0x" {
		uid, err := strconv.ParseUint(uidStr[2:], 16, 64)
		if err != nil {
			return 0, false
		}
		return uid, true
	}

	return 0, false
}

func (engine *Engine) commitOrAbort(ctx context.Context, ns *Namespace, tc *api.TxnContext) (*api.TxnContext, error) {
	if tc.Aborted {
		return tc, nil
	}

	// For commit, use an empty mutation to trigger the commit mechanism
	emptyMutation := &api.Mutation{
		CommitNow: true,
	}

	_, err := ns.Mutate(ctx, []*api.Mutation{emptyMutation})
	if err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	return &api.TxnContext{
		StartTs:  tc.StartTs,
		CommitTs: tc.StartTs + 1,
	}, nil
}

func (engine *Engine) Load(ctx context.Context, schemaPath, dataPath string) error {
	return engine.db0.Load(ctx, schemaPath, dataPath)
}

func (engine *Engine) LoadData(inCtx context.Context, dataDir string) error {
	return engine.db0.LoadData(inCtx, dataDir)
}

// Close closes the dgdao instance.
func (engine *Engine) Close() {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	if !engine.isOpen.Load() {
		return
	}

	if !singleton.CompareAndSwap(true, false) {
		panic("dgdao instance was not properly opened")
	}

	engine.isOpen.Store(false)
	x.UpdateHealthStatus(false)
	hooks.Disable()
	posting.Cleanup()
	worker.State.Dispose()

	if runtime.GOOS == "windows" {
		runtime.GC()
		time.Sleep(200 * time.Millisecond)
	}
}

func (ns *Engine) reset() error {
	z, restart, err := newZero()
	if err != nil {
		return fmt.Errorf("error initializing zero: %w", err)
	}

	if !restart {
		if err := worker.ApplyInitialSchema(0, 1); err != nil {
			return fmt.Errorf("error applying initial schema: %w", err)
		}
	}

	if err := schema.LoadFromDb(context.Background()); err != nil {
		return fmt.Errorf("error loading schema: %w", err)
	}
	for _, pred := range schema.State().Predicates() {
		worker.InitTablet(pred)
	}

	ns.z = z
	return nil
}
