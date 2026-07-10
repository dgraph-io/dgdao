/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dolan-in/dgman/v2"
	"github.com/go-logr/logr"
	"github.com/go-playground/validator/v10"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client provides an interface for dgdao operations
type Client interface {
	// Insert adds a new object or slice of objects to the database.
	// The object must be a pointer to a struct with appropriate dgraph tags.
	Insert(context.Context, any) error

	// InsertRaw adds a new object or slice of objects to the database.
	// The object must be a pointer to a struct with appropriate dgraph tags.
	// This is a no-op for remote Dgraph clients. For local clients, this
	// function mutates the Dgraph engine directly. No unique checks are performed.
	// The `UID` field for any objects must be set using the Dgraph blank node
	// prefix concept (e.g. "_:user1") to allow the engine to generate a UID for the object.
	InsertRaw(context.Context, any) error

	// Upsert inserts an object if it doesn't exist or updates it if it does.
	// This operation requires a field with a unique directive in the dgraph tag.
	// If no predicates are specified, the first predicate with the `upsert` tag will be used.
	// If none are specified in the predicates argument, the first predicate with the `upsert` tag
	// will be used.
	Upsert(context.Context, any, ...string) error

	// LoadOrStore stores the object only if no node matches the upsert
	// predicates, returning loaded=true when an existing node already matched
	// (the object is then populated from it). Insert-if-absent.
	LoadOrStore(ctx context.Context, obj any, predicates ...string) (loaded bool, err error)

	// LoadAndDelete atomically reads the node whose key predicate equals key
	// into obj and deletes it, returning loaded=false when none matched.
	// Read-and-consume; concurrent callers elect one winner.
	LoadAndDelete(ctx context.Context, obj any, key any, predicates ...string) (loaded bool, err error)

	// Update modifies an existing object in the database.
	// The object must be a pointer to a struct and must have a UID field set.
	Update(context.Context, any) error

	// Get retrieves a single object by its UID and populates the provided object.
	// The object parameter must be a pointer to a struct.
	Get(context.Context, any, string) error

	// Query creates a new query builder for retrieving data from the database.
	// Returns a *dgman.Query that can be further refined with filters, pagination, etc.
	Query(context.Context, any) *dgman.Query

	// Delete removes objects with the specified UIDs from the database.
	Delete(context.Context, []string) error

	// Close releases all resources used by the client.
	// It should be called when the client is no longer needed.
	Close()

	// UpdateSchema ensures the database schema matches the provided object types.
	// Pass one or more objects that will be used as templates for the schema.
	UpdateSchema(context.Context, ...any) error

	// AlterSchema applies a raw Dgraph Schema Definition Language string directly,
	// bypassing the object-template inference of UpdateSchema. Use it when you need
	// full control over predicate types, indexes, and directives — for example,
	// schema migrations that declare predicates no Go type models yet.
	AlterSchema(ctx context.Context, schema string) error

	// GetSchema retrieves the current schema definition from the database.
	// Returns a string containing the full schema in Dgraph Schema Definition Language.
	GetSchema(context.Context) (string, error)

	// DropAll removes the schema and all data from the database.
	DropAll(context.Context) error

	// DropData removes all data from the database but keeps the schema intact.
	DropData(context.Context) error

	// QueryRaw executes a raw Dgraph query with optional query variables.
	// The `query` parameter is the Dgraph query string.
	// The `vars` parameter is a map of variable names to their values, used to parameterize the query.
	QueryRaw(context.Context, string, map[string]string) ([]byte, error)

	// DgraphClient returns a gRPC Dgraph client from the connection pool and a cleanup function.
	// The cleanup function must be called when finished with the client to return it to the pool.
	DgraphClient() (*dgo.Dgraph, func(), error)

	// WithRetry executes fn, retrying on aborted transactions per policy.
	WithRetry(ctx context.Context, policy RetryPolicy, fn func() error) error
}

const (
	// dgraphURIPrefix is the prefix for Dgraph server connections
	dgraphURIPrefix = "dgraph://"

	// fileURIPrefix is the prefix for file-based local connections
	fileURIPrefix = "file://"
)

var (
	clientMap     = make(map[string]Client)
	clientMapLock sync.RWMutex
)

// StructValidator is the interface for struct validation.
// This is compatible with github.com/go-playground/validator/v10.Validate.
type StructValidator interface {
	// StructCtx validates a struct with context support.
	StructCtx(ctx context.Context, s interface{}) error
}

// SelfValidator lets a type drive its own validation. When a value passed to
// Insert, Upsert, or Update implements SelfValidator, the client calls
// ValidateWith instead of handing the value straight to the configured
// StructValidator.
//
// This is the seam for validation that struct tags cannot express on their own:
// cross-field rules (one field constrained by another), conditional rules,
// checks on computed or setter-derived values, and broader business rules.
// ValidateWith receives the configured StructValidator, so an implementation can
// still run ordinary tag-based validation and then layer custom logic on top.
type SelfValidator interface {
	ValidateWith(ctx context.Context, v StructValidator) error
}

// Defaulter lets a model populate default field values before a write.
// dgdao calls ApplyDefaults on the model in Insert, Upsert, Update, and
// LoadOrStore, before validation, so a defaulted field can satisfy a
// validate:"required" rule. Implementations set a field only when it is
// the zero value, except monotonic fields set on every write.
type Defaulter interface {
	ApplyDefaults(ctx context.Context) error
}

// clientOptions holds configuration options for the client.
//
// autoSchema: whether to automatically manage the schema.
// poolSize: the size of the dgo client connection pool.
// maxEdgeTraversal: the maximum number of edges to traverse when querying.
// namespace: the namespace for the client.
// logger: the logger for the client.
// validator: the validator instance for struct validation.
// embeddingProvider: optional provider for automatic SimString vector embeddings.
type clientOptions struct {
	autoSchema        bool
	poolSize          int
	maxEdgeTraversal  int
	cacheSizeMB       int
	maxRecvMsgSize    int
	grpcDialOptions   []grpc.DialOption
	namespace         string
	logger            logr.Logger
	validator         StructValidator
	embeddingProvider EmbeddingProvider
}

// ClientOpt is a function that configures a client
type ClientOpt func(*clientOptions)

// WithAutoSchema enables automatic schema management
func WithAutoSchema(enable bool) ClientOpt {
	return func(o *clientOptions) {
		o.autoSchema = enable
	}
}

// WithPoolSize sets the size of the dgraph client connection pool
func WithPoolSize(size int) ClientOpt {
	return func(o *clientOptions) {
		o.poolSize = size
	}
}

// WithNamespace sets the namespace for the client
func WithNamespace(namespace string) ClientOpt {
	return func(o *clientOptions) {
		o.namespace = namespace
	}
}

// WithLogger sets a structured logger for the client
func WithLogger(logger logr.Logger) ClientOpt {
	return func(o *clientOptions) {
		o.logger = logger
	}
}

// WithMaxEdgeTraversal sets the maximum depth of edges to traverse when fetching an object
func WithMaxEdgeTraversal(max int) ClientOpt {
	return func(o *clientOptions) {
		o.maxEdgeTraversal = max
	}
}

// WithCacheSizeMB sets the memory cache size in MB (only applicable for embedded databases).
// A good starting point for a system with a moderate amount of RAM (e.g., 8-16GB) would be
// between 256 MB and 1 GB. Dgraph itself often defaults to a 1GB cache. In order to minimize
// resource usage sans configuration, the default is set to 64 MB.
func WithCacheSizeMB(size int) ClientOpt {
	return func(o *clientOptions) {
		o.cacheSizeMB = size
	}
}

// WithMaxRecvMsgSize sets the maximum gRPC receive message size in bytes for
// remote Dgraph connections. When greater than zero, the client is constructed
// via dgo.NewClient with grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(size))
// applied so large query responses (e.g. wide subgraphs or vector results) do
// not exceed the default 4 MB gRPC limit. Ignored for embedded (file://) URIs.
func WithMaxRecvMsgSize(size int) ClientOpt {
	return func(o *clientOptions) {
		o.maxRecvMsgSize = size
	}
}

// WithGRPCDialOption appends a custom grpc.DialOption applied when opening a
// remote (dgraph://) connection. It is the general escape hatch for gRPC dial
// settings the dedicated options do not cover — TLS transport credentials,
// interceptors, keepalive parameters, and so on. May be supplied multiple
// times; the options are applied in the order given, after any option implied
// by WithMaxRecvMsgSize. Ignored for embedded (file://) URIs.
func WithGRPCDialOption(opt grpc.DialOption) ClientOpt {
	return func(o *clientOptions) {
		o.grpcDialOptions = append(o.grpcDialOptions, opt)
	}
}

// WithValidator sets a validator instance for struct validation.
// The validator will be used to validate structs before insert, upsert, and update operations.
// If no validator is provided, validation will be skipped.
// Any type implementing StructValidator can be used, including *validator.Validate from
// github.com/go-playground/validator/v10.
func WithValidator(v StructValidator) ClientOpt {
	return func(o *clientOptions) {
		o.validator = v
	}
}

// WithEmbeddingProvider sets the EmbeddingProvider used to automatically generate
// and maintain shadow float32vector predicates for SimString fields tagged with
// `dgraph:"embedding"`. When set, Insert, Upsert, and Update operations will
// call the provider to embed any SimString values and persist the resulting
// vectors alongside the primary string predicates.
func WithEmbeddingProvider(p EmbeddingProvider) ClientOpt {
	return func(o *clientOptions) {
		o.embeddingProvider = p
	}
}

// NewValidator creates a new validator instance with default settings.
// This is a convenience function for creating a validator to use with WithValidator.
// It returns a *validator.Validate from github.com/go-playground/validator/v10.
func NewValidator() *validator.Validate {
	return validator.New()
}

// NewClient creates a new graph database client instance based on the provided URI.
//
// The function supports two URI schemes:
//   - dgraph://host:port - Connects to a remote Dgraph instance
//   - file:///path/to/db - Creates or opens a local file-based database
//
// Optional configuration can be provided via the opts parameter:
//   - WithAutoSchema(bool) - Enable/disable automatic schema creation for inserted objects
//   - WithPoolSize(int) - Set the connection pool size for better performance under load
//   - WithMaxEdgeTraversal(int) - Set the maximum number of edges to traverse when fetching an object
//   - WithNamespace(string) - Set the database namespace for multi-tenant installations
//   - WithLogger(logr.Logger) - Configure structured logging with custom verbosity levels
//   - WithCacheSizeMB(int) - Set the memory cache size in MB (only applicable for embedded databases)
//   - WithValidator(*validator.Validate) - Set a validator instance for struct validation before mutations
//
// The returned Client provides a consistent interface regardless of whether you're
// connected to a remote Dgraph cluster or a local embedded database. This abstraction
// helps prevent connection issues and provides seamless access to embedded Dgraph.
//
// For file-based URIs, the client maintains a singleton Engine instance to ensure
// data consistency across multiple client connections to the same database.
func NewClient(uri string, opts ...ClientOpt) (Client, error) {
	// Default options
	options := clientOptions{
		autoSchema:       false,
		poolSize:         10,
		namespace:        "",
		maxEdgeTraversal: 10,
		cacheSizeMB:      64,             // 64 MB
		logger:           logr.Discard(), // No-op logger by default
	}

	// Apply provided options
	for _, opt := range opts {
		opt(&options)
	}

	// TODO: implement namespace support for v25
	if options.namespace != "" {
		options.logger.Info("Warning, namespace is set, but it is not supported in this version")
	}

	client := client{
		uri:       uri,
		options:   options,
		logger:    options.logger,
		consumeMu: &sync.Mutex{},
	}

	clientMapLock.Lock()
	defer clientMapLock.Unlock()
	key := client.key()
	if _, ok := clientMap[key]; ok {
		return clientMap[key], nil
	}

	switch {
	case strings.HasPrefix(uri, dgraphURIPrefix):
		factory := func() (*dgo.Dgraph, error) {
			client.logger.V(2).Info("Opening new Dgraph connection", "uri", uri)
			return dgo.Open(uri)
		}
		// Assemble any custom gRPC dial options. maxRecvMsgSize is folded
		// into the same mechanism as WithGRPCDialOption so the two compose.
		var dialOpts []grpc.DialOption
		if options.maxRecvMsgSize > 0 {
			dialOpts = append(dialOpts,
				grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(options.maxRecvMsgSize)))
		}
		dialOpts = append(dialOpts, options.grpcDialOptions...)
		if len(dialOpts) > 0 {
			endpoint, dgoOpts, err := parseDgraphURI(uri)
			if err != nil {
				return nil, err
			}
			for _, opt := range dialOpts {
				dgoOpts = append(dgoOpts, dgo.WithGrpcOption(opt))
			}
			factory = func() (*dgo.Dgraph, error) {
				client.logger.V(2).Info("Opening new Dgraph connection",
					"uri", uri, "maxRecvMsgSize", options.maxRecvMsgSize,
					"grpcDialOptions", len(options.grpcDialOptions))
				return dgo.NewClient(endpoint, dgoOpts...)
			}
		}
		client.pool = newClientPool(options.poolSize, factory, client.logger)
		dgman.SetLogger(client.logger)
		clientMap[key] = client
		return client, nil
	case strings.HasPrefix(uri, fileURIPrefix):
		// parse off the file:// prefix
		uri = uri[len(fileURIPrefix):]
		if _, err := os.Stat(uri); err != nil {
			return nil, err
		}
		engine, err := NewEngine(Config{
			dataDir:     uri,
			logger:      client.logger,
			cacheSizeMB: options.cacheSizeMB,
		})
		if err != nil {
			return nil, err
		}
		client.engine = engine
		// Create embedded dgo client that routes to engine
		ns := engine.GetDefaultNamespace()
		if options.namespace != "" {
			nsID, err := parseNamespaceID(options.namespace)
			if err != nil {
				engine.Close()
				return nil, fmt.Errorf("invalid namespace ID %q: %w", options.namespace, err)
			}
			ns, err = engine.GetNamespace(nsID)
			if err != nil {
				engine.Close()
				return nil, fmt.Errorf("failed to get namespace %d: %w", nsID, err)
			}
		}
		client.pool = newClientPool(1, func() (*dgo.Dgraph, error) {
			embeddedClient := newEmbeddedDgraphClient(engine, ns)
			//nolint:staticcheck // dgo.NewDgraphClient is deprecated but required for embedded client
			return dgo.NewDgraphClient(embeddedClient), nil
		}, client.logger)
		dgman.SetLogger(client.logger)
		clientMap[key] = client
		return client, nil
	}
	return nil, errors.New("invalid uri")

}

// parseDgraphURI mirrors dgo.Open's connection-string parsing so callers can
// route through dgo.NewClient with additional dgo.ClientOption values (e.g.
// custom grpc.DialOption settings). It returns the host:port endpoint and the
// dgo client options derived from the URI's auth, sslmode, and namespace params.
func parseDgraphURI(connStr string) (string, []dgo.ClientOption, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return "", nil, fmt.Errorf("invalid connection string: %w", err)
	}
	if u.Scheme != "dgraph" {
		return "", nil, errors.New("invalid scheme: must start with dgraph://")
	}
	if !strings.Contains(u.Host, ":") {
		return "", nil, errors.New("invalid connection string: host url must have both host and port")
	}
	if strings.Split(u.Host, ":")[1] == "" {
		return "", nil, errors.New("invalid connection string: missing port after port-separator colon")
	}

	params, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", nil, fmt.Errorf("malformed connection string: %w", err)
	}

	apiKey := params.Get("apikey")
	bearerToken := params.Get("bearertoken")
	if apiKey != "" && bearerToken != "" {
		return "", nil, errors.New("invalid connection string: both apikey and bearertoken cannot be provided")
	}

	opts := []dgo.ClientOption{}
	if apiKey != "" {
		opts = append(opts, dgo.WithDgraphAPIKey(apiKey))
	}
	if bearerToken != "" {
		opts = append(opts, dgo.WithBearerToken(bearerToken))
	}

	sslMode := params.Get("sslmode")
	if sslMode == "" {
		sslMode = "disable"
	}
	switch sslMode {
	case "disable":
		opts = append(opts, dgo.WithGrpcOption(
			grpc.WithTransportCredentials(insecure.NewCredentials())))
	case "require":
		opts = append(opts, dgo.WithSkipTLSVerify())
	case "verify-ca":
		opts = append(opts, dgo.WithSystemCertPool())
	default:
		return "", nil, fmt.Errorf(
			"invalid SSL mode: %s (must be one of disable, require, verify-ca)", sslMode)
	}

	if nsParam := params.Get("namespace"); nsParam != "" {
		nsID, err := strconv.ParseUint(nsParam, 10, 64)
		if err != nil {
			return "", nil, fmt.Errorf("invalid namespace ID: %w", err)
		}
		opts = append(opts, dgo.WithNamespace(nsID))
	}

	if u.User != nil {
		username := u.User.Username()
		password, _ := u.User.Password()
		if username == "" || password == "" {
			return "", nil, errors.New("invalid connection string: both username and password must be provided")
		}
		opts = append(opts, dgo.WithACLCreds(username, password))
	}

	return u.Host, opts, nil
}

type client struct {
	uri     string
	engine  *Engine
	options clientOptions
	pool    *clientPool
	logger  logr.Logger
	// consumeMu serializes LoadAndDelete's read-then-delete critical section so
	// exactly one in-process caller consumes a given node. The client value is
	// copied (value receivers, cached by value in clientMap), so the mutex is a
	// pointer shared across every copy that shares this client's connection.
	// Against a real Dgraph cluster the shared read-write transaction would also
	// abort losers on commit conflict; this lock additionally guarantees
	// single-winner semantics against the embedded engine, whose commit path
	// performs no optimistic-concurrency conflict check.
	consumeMu *sync.Mutex
}

func (c client) key() string {
	validatorKey := "nil"
	if c.options.validator != nil {
		validatorKey = fmt.Sprintf("%p", c.options.validator)
	}
	embeddingKey := "nil"
	if c.options.embeddingProvider != nil {
		embeddingKey = fmt.Sprintf("%p", c.options.embeddingProvider)
	}
	// Custom gRPC dial options only apply to remote (dgraph://) connections;
	// they are ignored for embedded (file://) URIs, so they only contribute to
	// the dedup key for remote clients — matching that documented behavior.
	dialKey := "0"
	if strings.HasPrefix(c.uri, dgraphURIPrefix) {
		dialKey = dialOptionsKey(c.options.grpcDialOptions)
	}
	return fmt.Sprintf("%s:%t:%d:%d:%d:%d:%s:%s:%s:%s", c.uri, c.options.autoSchema, c.options.poolSize,
		c.options.maxEdgeTraversal, c.options.cacheSizeMB, c.options.maxRecvMsgSize,
		c.options.namespace, validatorKey, embeddingKey, dialKey)
}

// dialOptionsKey identifies a set of custom gRPC dial options for the client
// dedup cache. grpc.DialOption values are opaque and not comparable, so the key
// uses each option's runtime identity rather than just the count: two clients
// configured with different options get different keys and are never merged.
// Two clients built from separately-constructed but equivalent options also
// differ, which is safe — the cache errs toward keeping them apart rather than
// merging connections that were configured differently.
func dialOptionsKey(opts []grpc.DialOption) string {
	if len(opts) == 0 {
		return "0"
	}
	parts := make([]string, len(opts))
	for i, opt := range opts {
		parts[i] = fmt.Sprintf("%p", opt)
	}
	return strings.Join(parts, ",")
}

// embeddingProvider implements the embeddingClient interface, exposing the
// configured EmbeddingProvider to package-level helpers like SimilarToText.
func (c client) embeddingProvider() EmbeddingProvider {
	return c.options.embeddingProvider
}

func checkPointer(obj any) error {
	// reflect.ValueOf(nil) yields a zero Value whose Kind is Invalid, so this is
	// nil-safe; reflect.TypeOf(nil).Kind() would instead panic on a nil receiver.
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Pointer {
		return errors.New("object must be a pointer")
	}
	// A typed nil pointer ((*T)(nil)) has Kind Ptr but nothing to read or write;
	// downstream reflection (uidOf, dgman) would panic on it, so reject it here.
	if v.IsNil() {
		return errors.New("object must not be a nil pointer")
	}
	return nil
}

// applyDefaults invokes ApplyDefaults on model if it (or its address)
// implements Defaulter, mirroring validateOne's addressable-pointer dispatch
// so pointer-receiver implementations are found regardless of whether model
// is passed as a struct value or a pointer. It is a no-op for a value that
// does not implement Defaulter, and for a nil pointer.
func (c client) applyDefaults(ctx context.Context, model any) error {
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Pointer {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}
	iface := val.Interface()
	if val.CanAddr() {
		iface = val.Addr().Interface()
	}
	if d, ok := iface.(Defaulter); ok {
		return d.ApplyDefaults(ctx)
	}
	return nil
}

// validateStruct validates a struct using the configured validator.
//
// It does NOT early-return when no StructValidator is configured: a value may
// implement SelfValidator, which must run on the default client too. The
// StructValidator fallback in validateOne is guarded by the nil check instead,
// so self-validation fires regardless of whether a StructValidator was set.
func (c client) validateStruct(ctx context.Context, obj any) error {
	// Handle both single structs and slices
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Pointer {
		if val.IsNil() {
			return fmt.Errorf("cannot validate nil pointer")
		}
		val = val.Elem()
	}

	if val.Kind() == reflect.Slice {
		for i := 0; i < val.Len(); i++ {
			elem := val.Index(i)
			if elem.Kind() == reflect.Pointer {
				if elem.IsNil() {
					return fmt.Errorf("cannot validate nil pointer at index %d", i)
				}
				elem = elem.Elem()
			}
			if err := c.validateOne(ctx, elem); err != nil {
				return err
			}
		}
	} else {
		return c.validateOne(ctx, val)
	}

	return nil
}

// validateOne validates a single struct value. If the value (or its address)
// implements SelfValidator, validation is delegated to ValidateWith so the type
// can apply custom rules — cross-field, conditional, computed-value, or other
// logic beyond struct tags. Otherwise the value is validated by the configured
// StructValidator as usual.
func (c client) validateOne(ctx context.Context, val reflect.Value) error {
	iface := val.Interface()
	if val.CanAddr() {
		iface = val.Addr().Interface()
	}
	if sv, ok := iface.(SelfValidator); ok {
		return sv.ValidateWith(ctx, c.options.validator)
	}
	if c.options.validator == nil {
		return nil // No StructValidator configured and not self-validating.
	}
	return c.options.validator.StructCtx(ctx, iface)
}

// Insert implements inserting an object or slice of objects in the database.
// Passed object must be a pointer to a struct with appropriate dgraph tags.
func (c client) Insert(ctx context.Context, obj any) error {
	obj = UnwrapSchema(obj)
	// Apply defaults before validation, so a defaulted field can satisfy a
	// validate:"required" rule.
	if err := c.applyDefaults(ctx, obj); err != nil {
		return err
	}
	// Validate struct before insertion
	if err := c.validateStruct(ctx, obj); err != nil {
		return err
	}

	return c.process(ctx, obj, "Insert", func(tx *dgman.TxnContext, obj any) ([]string, error) {
		return tx.MutateBasic(obj)
	})
}

// InsertRaw adds a new object or slice of objects to the database.
// The object must be a pointer to a struct with appropriate dgraph tags.
// The `UID` field for any objects must be set using the Dgraph blank node
// prefix concept (e.g. "_:user1") to allow the engine to generate a UID for the object.
//
// Deprecated: InsertRaw is now identical to Insert. Use Insert instead.
func (c client) InsertRaw(ctx context.Context, obj any) error {
	obj = UnwrapSchema(obj)
	// Validate struct before insertion
	if err := c.validateStruct(ctx, obj); err != nil {
		return err
	}

	return c.process(ctx, obj, "Insert", func(tx *dgman.TxnContext, obj any) ([]string, error) {
		return tx.MutateBasic(obj)
	})
}

// Upsert implements inserting or updating an object or slice of objects in the database.
// Note that the struct tag `upsert` must be used. One or more predicates can be specified
// to be used for upserting. If none are specified, the first predicate with the `upsert` tag
// will be used.
func (c client) Upsert(ctx context.Context, obj any, predicates ...string) error {
	obj = UnwrapSchema(obj)
	// Apply defaults before validation, so a defaulted field can satisfy a
	// validate:"required" rule.
	if err := c.applyDefaults(ctx, obj); err != nil {
		return err
	}
	// Validate struct before upsert
	if err := c.validateStruct(ctx, obj); err != nil {
		return err
	}

	return c.process(ctx, obj, "Upsert", func(tx *dgman.TxnContext, obj any) ([]string, error) {
		return tx.Upsert(obj, predicates...)
	})
}

// LoadOrStore stores obj only if no node already matches the upsert predicates,
// reporting whether one already existed (loaded == true). Built on dgman
// MutateOrGet, which returns the UIDs of newly created nodes only: an empty
// result means an existing node matched, and obj is populated with its fields.
// With no predicates, the first field tagged dgraph:"upsert" is used.
func (c client) LoadOrStore(ctx context.Context, obj any, predicates ...string) (loaded bool, err error) {
	obj = UnwrapSchema(obj)
	// Reject nil / non-pointer input up front, as LoadAndDelete and Get do:
	// validateStruct is a no-op when no validator is configured, so without this
	// a typed nil or non-pointer would reach MutateOrGet and panic or silently
	// fail to hydrate on the loaded=true path.
	if err := checkPointer(obj); err != nil {
		return false, err
	}
	// Apply defaults before validation, so a defaulted field can satisfy a
	// validate:"required" rule.
	if err := c.applyDefaults(ctx, obj); err != nil {
		return false, err
	}
	if err := c.validateStruct(ctx, obj); err != nil {
		return false, err
	}

	dgClient, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return false, err
	}
	defer c.pool.put(dgClient)

	tx := dgman.NewTxnContext(ctx, dgClient).SetCommitNow()
	uids, err := tx.MutateOrGet(obj, predicates...)
	if err != nil {
		if uniqueErr := parseUniqueError(err); uniqueErr != nil {
			return false, uniqueErr
		}
		return false, err
	}
	// MutateOrGet returns created UIDs only; empty => an existing node matched.
	return len(uids) == 0, nil
}

// firstUpsertPredicate returns the Dgraph predicate name of the first field
// tagged dgraph:"...upsert...". The predicate defaults to the json tag name
// unless an explicit predicate= token is present. It returns "" if no upsert
// field exists.
func firstUpsertPredicate(obj any) string {
	v := reflect.ValueOf(obj)
	for v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	// A nil pointer (or nil interface) dereferences to an invalid Value, and a
	// non-struct has no fields; either way there is no upsert predicate, and
	// calling Type()/NumField() on them would panic.
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return ""
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		dgTag := f.Tag.Get("dgraph")
		if !strings.Contains(dgTag, "upsert") {
			continue
		}
		// Explicit predicate=<name> wins.
		for _, directive := range strings.Fields(dgTag) {
			if strings.HasPrefix(directive, "predicate=") {
				return strings.TrimPrefix(directive, "predicate=")
			}
		}
		// Otherwise fall back to the json tag name.
		if jsonTag := f.Tag.Get("json"); jsonTag != "" {
			return strings.Split(jsonTag, ",")[0]
		}
		return f.Name
	}
	return ""
}

// isValidPredicateName reports whether pred is safe to concatenate into a DQL
// filter. LoadAndDelete builds "eq(<pred>, $1)" by string concatenation; the key
// value is parameterized, but the predicate name is not, so a name containing DQL
// metacharacters (parentheses, commas, whitespace, quotes, angle brackets, ...)
// could corrupt or inject the query. Dgraph predicate names are plain identifiers
// over letters, digits, '_', '.', and '-'; anything outside that set is rejected
// rather than trusted.
func isValidPredicateName(pred string) bool {
	if pred == "" {
		return false
	}
	for _, r := range pred {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '_', r == '.', r == '-':
		default:
			return false
		}
	}
	return true
}

// zeroValue resets the value pointed to by obj to its zero value. LoadAndDelete
// promises obj is left zero when it returns loaded=false, but tx.Get hydrates obj
// on a read whose commit may then abort and retry into not-found; without this
// reset the caller would observe stale data from the aborted attempt (or their
// own pre-populated fields). obj is a non-nil pointer here (checkPointer ran).
func zeroValue(obj any) {
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return
	}
	if e := v.Elem(); e.CanSet() {
		e.Set(reflect.Zero(e.Type()))
	}
}

// uidOf reflects out the UID field of a dgraph struct pointer.
func uidOf(obj any) string {
	v := reflect.ValueOf(obj)
	for v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	// A nil pointer dereferences to an invalid Value and a non-struct has no
	// fields; FieldByName would panic on either, so guard as firstUpsertPredicate
	// does. (LoadAndDelete already rejects nil via checkPointer; this is
	// defense-in-depth for any other caller.)
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return ""
	}
	f := v.FieldByName("UID")
	if f.IsValid() && f.Kind() == reflect.String {
		return f.String()
	}
	return ""
}

// LoadAndDelete atomically reads the node whose key predicate equals key into
// obj and deletes it, returning loaded=false (and leaving obj zero) when no
// node matched. The read and delete share one transaction with no CommitNow,
// so two concurrent callers conflict on commit: exactly one wins (loaded=true),
// the loser aborts and retries into not-found (loaded=false). This reproduces
// PostgreSQL's DELETE … RETURNING. With no predicates, the first dgraph:"upsert"
// field is used.
func (c client) LoadAndDelete(ctx context.Context, obj any, key any, predicates ...string) (loaded bool, err error) {
	obj = UnwrapSchema(obj)
	if err := checkPointer(obj); err != nil {
		return false, err
	}

	pred := ""
	if len(predicates) > 0 {
		pred = predicates[0]
	} else {
		pred = firstUpsertPredicate(obj)
	}
	if pred == "" {
		return false, fmt.Errorf("LoadAndDelete: no key predicate (pass one or tag a field dgraph:\"upsert\")")
	}
	// The key value below is parameterized ($1), but the predicate name is
	// concatenated straight into the DQL filter, so a name carrying DQL
	// metacharacters could corrupt or inject the query. Reject anything that is
	// not a plain Dgraph predicate identifier before it reaches the filter.
	if !isValidPredicateName(pred) {
		return false, fmt.Errorf("LoadAndDelete: invalid key predicate %q (allowed: letters, digits, '_', '.', '-')", pred)
	}

	dgClient, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return false, err
	}
	defer c.pool.put(dgClient)

	// Serialize the read-then-delete critical section across in-process callers.
	// The shared read-write transaction already elects one winner against a real
	// Dgraph cluster (the loser aborts on commit), but the embedded engine does
	// no commit-time conflict check, so without this lock concurrent callers
	// would each read the node and each report loaded=true. A real Dgraph
	// cluster aborts the loser of a commit conflict and the bounded retry below
	// already elects a single winner, so the lock is needed — and taken — only
	// for the embedded engine. Taking it for remote clusters would needlessly
	// serialize consumers operating on unrelated keys.
	if c.engine != nil && c.consumeMu != nil {
		c.consumeMu.Lock()
		defer c.consumeMu.Unlock()
	}

	// Bounded retry: Dgraph aborts the loser of a commit conflict; the retry
	// reads the node already gone and reports not-found.
	const maxAttempts = 10
	for attempt := 0; ; attempt++ {
		tx := dgman.NewTxnContext(ctx, dgClient)
		getErr := tx.Get(obj).
			Filter("eq("+pred+", $1)", key).
			All(c.options.maxEdgeTraversal).
			Node()
		if getErr != nil {
			_ = tx.Discard()
			// dgman returns ErrNodeNotFound when nothing matches.
			if errors.Is(getErr, dgman.ErrNodeNotFound) {
				// Honor the documented contract: obj is zero when loaded=false.
				// A prior attempt's Get (before a commit abort) may have hydrated
				// obj, and the caller may have passed a pre-populated struct.
				zeroValue(obj)
				return false, nil
			}
			return false, getErr
		}

		uid := uidOf(obj)
		if uid == "" {
			// A genuine miss already returned via ErrNodeNotFound above, so the
			// Get here matched and hydrated a node. An empty UID therefore means
			// the model has no readable string UID field, not that nothing existed
			// -- reporting loaded=false would silently skip a matched node, so
			// surface it as an error instead.
			_ = tx.Discard()
			return false, fmt.Errorf("LoadAndDelete: matched a node but read no UID; the model needs a string UID field")
		}

		if delErr := tx.DeleteNode(uid); delErr != nil {
			_ = tx.Discard()
			return false, delErr
		}

		if cErr := tx.Commit(); cErr != nil {
			_ = tx.Discard()
			if isAbortedErr(cErr) {
				// Lost the race or a concurrent change; retry — the winner has
				// already deleted the node, so the retry reads not-found.
				if attempt < maxAttempts {
					continue
				}
			}
			return false, cErr
		}
		return true, nil
	}
}

// isAbortedErr reports whether err is a Dgraph transaction-conflict abort,
// matching both dgo's ErrAborted sentinel and the underlying message in case a
// wrapped or stringified form reaches us.
func isAbortedErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, dgo.ErrAborted) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "aborted")
}

// Update implements updating an existing object in the database.
// Passed object must be a pointer to a struct.
func (c client) Update(ctx context.Context, obj any) error {
	obj = UnwrapSchema(obj)
	// Apply defaults before validation, so a defaulted field can satisfy a
	// validate:"required" rule.
	if err := c.applyDefaults(ctx, obj); err != nil {
		return err
	}
	// Validate struct before update
	if err := c.validateStruct(ctx, obj); err != nil {
		return err
	}

	return c.process(ctx, obj, "Update", func(tx *dgman.TxnContext, obj any) ([]string, error) {
		return tx.MutateBasic(obj)
	})
}

// Delete implements removing objects with the specified UIDs.
func (c client) Delete(ctx context.Context, uids []string) error {
	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return err
	}
	defer c.pool.put(client)

	txn := dgman.NewTxnContext(ctx, client).SetCommitNow()
	return txn.DeleteNode(uids...)
}

// Get implements retrieving a single object by its UID.
// Passed object must be a pointer to a struct.
func (c client) Get(ctx context.Context, obj any, uid string) error {
	obj = UnwrapSchema(obj)
	err := checkPointer(obj)
	if err != nil {
		return err
	}

	client, err := c.pool.get()
	if err != nil {
		return err
	}
	defer c.pool.put(client)

	txn := dgman.NewReadOnlyTxnContext(ctx, client)
	return txn.Get(obj).UID(uid).All(c.options.maxEdgeTraversal).Node()
}

// Returns a *dgman.Query that can be further refined with filters, pagination, etc.
// The returned query will be limited to the maximum number of edges specified in the options.
func (c client) Query(ctx context.Context, model any) *dgman.Query {
	model = UnwrapSchema(model)
	client, err := c.pool.get()
	if err != nil {
		return nil
	}
	defer c.pool.put(client)

	txn := dgman.NewReadOnlyTxnContext(ctx, client)
	return txn.Get(model).All(c.options.maxEdgeTraversal)
}

// AlterSchema applies a raw DQL schema string directly via Dgraph Alter,
// without the object-template inference performed by UpdateSchema.
func (c client) AlterSchema(ctx context.Context, schema string) error {
	dgClient, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return err
	}
	defer c.pool.put(dgClient)

	return dgClient.Alter(ctx, &api.Operation{Schema: schema})
}

// UpdateSchema implements updating the Dgraph schema. Pass one or more
// objects that will be used to generate the schema.
// If any object contains SimString fields tagged `dgraph:"embedding"`, the
// corresponding shadow float32vector predicates (<field>__vec) are also registered.
func (c client) UpdateSchema(ctx context.Context, obj ...any) error {
	for i := range obj {
		obj[i] = UnwrapSchema(obj[i])
	}
	dgClient, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return err
	}
	defer c.pool.put(dgClient)

	if _, err = dgman.CreateSchema(dgClient, obj...); err != nil {
		return err
	}

	// Collect shadow vector schema lines for SimString fields across all objects.
	var vecSchema strings.Builder
	for _, o := range obj {
		for _, info := range collectSimFields(o) {
			vecSchema.WriteString(buildVecSchemaStatement(info))
			vecSchema.WriteString("\n")
		}
	}
	if vecSchema.Len() == 0 {
		return nil
	}

	return dgClient.Alter(ctx, &api.Operation{Schema: vecSchema.String()})
}

// GetSchema implements retrieving the Dgraph schema.
func (c client) GetSchema(ctx context.Context) (string, error) {
	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return "", err
	}
	defer c.pool.put(client)

	return dgman.GetSchema(client)
}

// DropAll implements dropping all data and schema from the database.
func (c client) DropAll(ctx context.Context) error {
	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return err
	}
	defer c.pool.put(client)

	return client.Alter(ctx, &api.Operation{DropAll: true})
}

// DropData implements dropping data from the database.
func (c client) DropData(ctx context.Context) error {
	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return err
	}
	defer c.pool.put(client)

	return client.Alter(ctx, &api.Operation{DropOp: api.Operation_DATA})
}

// QueryRaw implements raw querying (DQL syntax) and optional variables.
func (c client) QueryRaw(ctx context.Context, q string, vars map[string]string) ([]byte, error) {
	client, err := c.pool.get()
	if err != nil {
		c.logger.Error(err, "Failed to get client from pool")
		return nil, err
	}
	defer c.pool.put(client)

	txn := dgman.NewReadOnlyTxnContext(ctx, client)
	resp, err := txn.Txn().QueryWithVars(ctx, q, vars)
	if err != nil {
		return nil, err
	}
	return resp.GetJson(), nil
}

// Close releases resources used by the client.
func (c client) Close() {
	// Add nil check to prevent panic if pool is nil
	if c.pool != nil {
		c.pool.close()
	}
	if c.engine != nil {
		c.engine.Close()
	}
}

// DgraphClient returns a Dgraph client from the pool and a cleanup function to put it back.
//
// Usage:
//
//	client, cleanup, err := c.DgraphClient()
//	if err != nil { ... }
//	defer cleanup()
//
// The cleanup function is safe to call even if client is nil or err is not nil.
func (c client) DgraphClient() (client *dgo.Dgraph, cleanup func(), err error) {
	client, err = c.pool.get()
	cleanup = func() {
		if client != nil {
			c.pool.put(client)
		}
	}
	return client, cleanup, err
}

type clientPool struct {
	clients chan *dgo.Dgraph
	factory func() (*dgo.Dgraph, error)
	logger  logr.Logger
}

func newClientPool(size int, factory func() (*dgo.Dgraph, error), logger logr.Logger) *clientPool {
	return &clientPool{
		clients: make(chan *dgo.Dgraph, size),
		factory: factory,
		logger:  logger,
	}
}

func (p *clientPool) get() (*dgo.Dgraph, error) {
	// Try to reuse an existing client
	select {
	case client := <-p.clients:
		p.logger.V(2).Info("Reusing client from pool")
		return client, nil
	default:
		// No client in pool, fall through to create a new one
	}

	// Create a new client
	p.logger.V(2).Info("Creating new client")
	client, err := p.factory()
	if err != nil {
		p.logger.Error(err, "Failed to create new client")
	}
	return client, err
}

func (p *clientPool) put(client *dgo.Dgraph) {
	select {
	case p.clients <- client:
		p.logger.V(2).Info("Returned client to pool")
	default:
		// Pool is full, close the client
		p.logger.V(1).Info("Pool full, closing client")
		client.Close()
	}
}

func (p *clientPool) close() {
	count := 0
	for {
		select {
		case client, ok := <-p.clients:
			if !ok {
				return // channel is closed
			}
			client.Close()
			count++
		default:
			// No more clients in the channel
			p.logger.V(2).Info("Client pool closed", "closedConnections", count)
			return
		}
	}
}
