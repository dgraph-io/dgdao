<div align="center">

[![GitHub Repo stars](https://img.shields.io/github/stars/dgraph-io/dgdao)](https://github.com/dgraph-io/dgdao/stargazers)
[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/dgraph-io/dgdao)](https://github.com/dgraph-io/dgdao/commits/main/)

</div>

**dgdao is a high-performance, transactional database system.** It's designed to be type-first,
schema-agnostic, and portable. dgdao provides ORM-like mechanisms that make it simple to build new
apps, paired with support for advanced use cases through the Dgraph Query Language (DQL). A dynamic
schema allows for natural relations to be expressed in your data with performance that scales with
your use case.

dgdao is available as a Go package for running in-process, providing low-latency reads, writes, and
vector searches. We’ve made trade-offs to prioritize speed and simplicity. When runnning in-process,
dgdao internalizes Dgraph's server components, and data is written to a local file-based database.
dgdao also supports remote Dgraph servers, allowing you deploy your apps to any Dgraph cluster
simply by changing the connection string.

## Quickstart

```go
package main

import (
    "context"
    "fmt"
    "time"

    dg "github.com/dgraph-io/dgdao"
)

type TestEntity struct {
    Name        string    `json:"name,omitempty" dgraph:"index=exact"`
    Description string    `json:"description,omitempty" dgraph:"index=term"`
    CreatedAt   time.Time `json:"createdAt,omitempty"`

    // UID is a required field for nodes
    UID string `json:"uid,omitempty"`
    // DType is a required field for nodes, will get populated with the struct name
    DType []string `json:"dgraph.type,omitempty"`
}

func main() {
    // Use a file URI to connect to a in-process dgdao instance, ensure that the directory exists
    uri := "file:///tmp/dgdao"
    // Assigning a Dgraph URI will connect to a remote Dgraph server
    // uri := "dgraph://localhost:9080"

    client, err := dg.NewClient(uri, dg.WithAutoSchema(true))
    if err != nil {
        panic(err)
    }
    defer client.Close()

    entity := TestEntity{
        Name:        "Test Entity",
        Description: "This is a test entity",
        CreatedAt:   time.Now(),
    }

    ctx := context.Background()
    err = client.Insert(ctx, &entity)

    if err != nil {
        panic(err)
    }
    fmt.Println("Insert successful, entity UID:", entity.UID)

    // Query the entity
    var result TestEntity
    err = client.Get(ctx, &result, entity.UID)
    if err != nil {
        panic(err)
    }
    fmt.Println("Query successful, entity:", result.UID)
}
```

## Creating a Client

The `NewClient` function takes a URI and optional configuration options.

```go
client, err := dg.NewClient(uri)
if err != nil {
    panic(err)
}
defer client.Close()
```

### URI Options

dgdao supports two URI schemes for managing graph databases:

#### `file://` - Local File-Based Database

Connects to a database stored locally on the filesystem. This mode doesn't require a separate
database server and is perfect for development, testing, or embedded applications. The directory
must exist before connecting.

File-based databases do not support concurrent access from separate processes. Further, there can
only be one file-based client per process.

```go
// Connect to a local file-based database
client, err := dg.NewClient("file:///path/to/data")
```

#### `dgraph://` - Remote Dgraph Server

Connects to a Dgraph cluster. For more details on the Dgraph URI format, see the
[Dgraph Dgo documentation](https://github.com/dgraph-io/dgo#connection-strings).

```go
// Connect to a remote Dgraph server
client, err := dg.NewClient("dgraph://hostname:9080")
```

You can have multiple remote clients per process provided the URIs are distinct.

### Configuration Options

dgdao provides several configuration options that can be passed to the `NewClient` function:

#### WithAutoSchema(bool)

Enables or disables automatic schema management. When enabled, dgdao will automatically create and
update the graph database schema based on the struct tags of objects you insert.

```go
// Enable automatic schema management
client, err := dg.NewClient(uri, dg.WithAutoSchema(true))
```

#### WithSchemaCache(bool)

Enables or disables caching of the per-write schema check (enabled by default). Every mutation
performs a schema round-trip before writing: with AutoSchema enabled it re-applies the schema for
the object's type, and with AutoSchema disabled it fetches the full schema to verify the type
exists. Since neither outcome changes once it has succeeded, the client caches the result per node
type and skips the round-trip on subsequent writes — a significant saving for write-heavy workloads.
`DropAll` invalidates the cache. The trade-off is that schema changes made by other processes are
not re-detected for types already cached; disable the cache if your writes must observe external
schema changes on every mutation.

```go
// Re-check the schema on every write (pre-cache behavior)
client, err := dg.NewClient(uri, dg.WithSchemaCache(false))
```

#### WithPoolSize(int)

Sets the size of the connection pool for better performance under load. The default is 10
connections.

```go
// Set pool size to 20 connections
client, err := dg.NewClient(uri, dg.WithPoolSize(20))
```

#### WithMaxEdgeTraversal(int)

Sets the maximum number of edges to traverse when querying. The default is 10 edges.

```go
// Set max edge traversal to 20 edges
client, err := dg.NewClient(uri, dg.WithMaxEdgeTraversal(20))
```

#### WithLogger(logr.Logger)

Configures structured logging with custom verbosity levels. By default, logging is disabled.

```go
// Set up a logger
logger := logr.New(logr.Discard())
client, err := dg.NewClient(uri, dg.WithLogger(logger))
```

#### WithValidator(Validator)

Configures custom validation for entities before mutations. The validator is called during insert,
update, and upsert operations to ensure data integrity.

```go
import "github.com/go-playground/validator/v10"

type User struct {
    Name  string `json:"name" validate:"required,min=2,max=100"`
    Email string `json:"email" validate:"required,email"`
    Age   int    `json:"age" validate:"gte=0,lte=130"`
}

// Create a validator instance
validate := validator.New()

// You can also register custom validations if needed
validate.RegisterValidation("custom", func(fl validator.FieldLevel) bool {
    return fl.Field().String() == "custom_value"
})

// Create client with the validator
client, err := dg.NewClient(uri, dg.WithValidator(validate))
```

See the [validator test](validate_test.go) for more examples.

#### SelfValidator (self-driven validation)

Struct tags validate one field at a time. When a rule spans fields or needs real logic —
`End >= Start`, a field required only when another is set, or any business rule — a type can drive
its own validation by implementing `SelfValidator`:

```go
type SelfValidator interface {
    ValidateWith(ctx context.Context, v StructValidator) error
}
```

When a value passed to `Insert`, `Upsert`, or `Update` implements `SelfValidator`, the client calls
`ValidateWith` on it. The configured `StructValidator` is handed in, so an implementation can run
the ordinary tag-based checks first and then layer custom logic on top:

```go
type Event struct {
    UID   string   `json:"uid,omitempty"`
    DType []string `json:"dgraph.type,omitempty"`
    Name  string   `json:"name,omitempty"`
    Start int      `json:"start,omitempty"`
    End   int      `json:"end,omitempty"`
}

func (e *Event) ValidateWith(ctx context.Context, v dg.StructValidator) error {
    if v != nil {
        if err := v.StructCtx(ctx, e); err != nil { // tag-based checks first
            return err
        }
    }
    if e.End < e.Start { // then the cross-field rule
        return fmt.Errorf("event %q: End (%d) must be >= Start (%d)", e.Name, e.End, e.Start)
    }
    return nil
}
```

Plain structs are unaffected — they still flow through the configured `StructValidator` as before.

You can combine multiple options:

```go
// Using multiple configuration options
client, err := dg.NewClient(uri,
    dg.WithAutoSchema(true),
    dg.WithPoolSize(20),
    dg.WithLogger(logger))
```

## Defining Your Graph with Structs

dgdao uses Go structs to define your graph database schema. By adding `json` and `dgraph` tags to
your struct fields, you tell dgdao how to store and index your data in the graph database.

### Basic Structure

Every struct that represents a node in your graph must include a `UID` and `DType` field, for
example:

```go
type MyNode struct {
    // Your fields here with appropriate tags
    Name string `json:"name,omitempty" dgraph:"index=exact"`
    Description string `json:"description,omitempty" dgraph:"index=term"`
    CreatedAt time.Time `json:"createdAt,omitempty" dgraph:"index=day"`

    // These fields are required for Dgraph integration
    UID   string   `json:"uid,omitempty"`
    DType []string `json:"dgraph.type,omitempty"`
}
```

### `dgraph` Field Tags

dgdao uses struct tags to define how each field should be handled in the graph database:

| Directive     | Option     | Description                                                                                                                                                                                                                            | Example                                                                                 |
| ------------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| **index**     | exact      | Creates an exact-match index for string fields                                                                                                                                                                                         | Name string &#96;json:"name" dgraph:"index=exact"&#96;                                  |
|               | hash       | Creates a hash index (same as exact)                                                                                                                                                                                                   | Code string &#96;json:"code" dgraph:"index=hash"&#96;                                   |
|               | term       | Creates a term index for text search                                                                                                                                                                                                   | Description string &#96;json:"description" dgraph:"index=term"&#96;                     |
|               | fulltext   | Creates a full-text search index                                                                                                                                                                                                       | Content string &#96;json:"content" dgraph:"index=fulltext"&#96;                         |
|               | int        | Creates an index for integer fields                                                                                                                                                                                                    | Age int &#96;json:"age" dgraph:"index=int"&#96;                                         |
|               | geo        | Creates a geolocation index                                                                                                                                                                                                            | Location &#96;json:"location" dgraph:"index=geo"&#96;                                   |
|               | day        | Creates a day-based index for datetime fields                                                                                                                                                                                          | Created time.Time &#96;json:"created" dgraph:"index=day"&#96;                           |
|               | year       | Creates a year-based index for datetime fields                                                                                                                                                                                         | Birthday time.Time &#96;json:"birthday" dgraph:"index=year"&#96;                        |
|               | month      | Creates a month-based index for datetime fields                                                                                                                                                                                        | Hired time.Time &#96;json:"hired" dgraph:"index=month"&#96;                             |
|               | hour       | Creates an hour-based index for datetime fields                                                                                                                                                                                        | Login time.Time &#96;json:"login" dgraph:"index=hour"&#96;                              |
|               | hnsw       | Creates a vector similarity index                                                                                                                                                                                                      | Vector \*dgman.VectorFloat32 &#96;json:"vector" dgraph:"index=hnsw(metric:cosine)"&#96; |
| **type**      | geo        | Specifies a geolocation field                                                                                                                                                                                                          | Location &#96;json:"location" dgraph:"type=geo"&#96;                                    |
|               | datetime   | Specifies a datetime field                                                                                                                                                                                                             | CreatedAt time.Time &#96;json:"createdAt" dgraph:"type=datetime"&#96;                   |
|               | int        | Specifies an integer field                                                                                                                                                                                                             | Count int &#96;json:"count" dgraph:"type=int"&#96;                                      |
|               | float      | Specifies a floating-point field                                                                                                                                                                                                       | Price float64 &#96;json:"price" dgraph:"type=float"&#96;                                |
|               | bool       | Specifies a boolean field                                                                                                                                                                                                              | Active bool &#96;json:"active" dgraph:"type=bool"&#96;                                  |
|               | password   | Specifies a password field (stored securely)                                                                                                                                                                                           | Password string &#96;json:"password" dgraph:"type=password"&#96;                        |
| **count**     |            | Creates a count index                                                                                                                                                                                                                  | Visits int &#96;json:"visits" dgraph:"count"&#96;                                       |
| **unique**    |            | Enforces uniqueness for the field                                                                                                                                                                                                      | Email string &#96;json:"email" dgraph:"index=hash unique"&#96;                          |
| **upsert**    |            | Allows a field to be used in upsert operations                                                                                                                                                                                         | UserID string &#96;json:"userID" dgraph:"index=hash upsert"&#96;                        |
| **reverse**   |            | Creates a bidirectional edge                                                                                                                                                                                                           | Friends []\*Person &#96;json:"friends" dgraph:"reverse"&#96;                            |
| **lang**      |            | Enables multi-language support for the field                                                                                                                                                                                           | Description string &#96;json:"description" dgraph:"lang"&#96;                           |
| **embedding** |            | Marks a `SimString` field for automatic vector embedding. dgdao calls the configured `EmbeddingProvider` on insert/update and maintains a shadow `<field>__vec` predicate. Can be combined with `index=term` and other string indexes. | Description SimString &#96;json:"description" dgraph:"embedding,index=term"&#96;        |
|               | metric=    | HNSW index metric (default: `cosine`). Options: `cosine`, `euclidean`, `dotproduct`                                                                                                                                                    | Description SimString &#96;json:"description" dgraph:"embedding,metric=euclidean"&#96;  |
|               | exponent=  | HNSW index exponent controlling index size (default: `4`)                                                                                                                                                                              | Description SimString &#96;json:"description" dgraph:"embedding,exponent=5"&#96;        |
|               | threshold= | Minimum rune count required to embed. Texts shorter than this have their shadow vector deleted rather than left stale, preventing false positives. Default: `0` (always embed)                                                         | Description SimString &#96;json:"description" dgraph:"embedding,threshold=20"&#96;      |

### Relationships

Relationships between nodes are defined using struct pointers or slices of struct pointers:

```go
type Person struct {
    Name     string    `json:"name,omitempty" dgraph:"index=exact upsert"`
    Friends  []*Person `json:"friends,omitempty"`
    Manager  *Person   `json:"manager,omitempty"`

    UID   string   `json:"uid,omitempty"`
    DType []string `json:"dgraph.type,omitempty"`
}
```

### Reverse Edges

Reverse edges enable efficient bidirectional graph traversal. dgdao supports two patterns:

**1. Forward edges with automatic reverse indexing** - Use `dgraph:"reverse"` on a forward edge to
enable querying in both directions:

```go
type FoafPerson struct {
    UID       string        `json:"uid,omitempty"`
    Name      string        `json:"person_name,omitempty" dgraph:"index=term,hash"`
    Friends   []*FoafPerson `json:"friends,omitempty" dgraph:"reverse"`  // Forward edge with reverse index
    FriendsOf []*FoafPerson `json:"~friends,omitempty" dgraph:"reverse"` // Query reverse direction
    DType     []string      `json:"dgraph.type,omitempty"`
}
```

**2. Managed reverse edges** - Define relationships from the parent side using the `~predicate` JSON
tag prefix. When inserting, dgdao automatically creates the forward edges on child entities:

```go
// Child: Enrollment has forward edge to Course
type Enrollment struct {
    UID      string    `json:"uid,omitempty"`
    Grade    string    `json:"grade,omitempty"`
    InCourse []*Course `json:"in_course,omitempty" dgraph:"reverse"`
    DType    []string  `json:"dgraph.type,omitempty"`
}

// Parent: Course defines managed reverse edge to Enrollments
type Course struct {
    UID         string        `json:"uid,omitempty"`
    Name        string        `json:"course_name,omitempty" dgraph:"index=term"`
    Enrollments []*Enrollment `json:"~in_course,omitempty" dgraph:"reverse"` // Managed reverse edge
    DType       []string      `json:"dgraph.type,omitempty"`
}
```

With managed reverse edges, you can insert from the parent and dgdao handles the edge direction
automatically:

```go
course := &Course{
    Name: "Algorithms",
    Enrollments: []*Enrollment{
        {Grade: "A"},
        {Grade: "B"},
    },
}
client.Insert(ctx, course)
// Creates: Enrollment1.in_course = [Course.UID], Enrollment2.in_course = [Course.UID]
```

See [reverse_test.go](./reverse_test.go) for comprehensive examples including multi-level
hierarchies and friend-of-a-friend patterns.

## Basic Operations

dgdao provides a simple API for common database operations.

### Inserting Data

To insert a new node into the database:

```go
ctx := context.Background()

// Create a new object
user := User{
    Name:  "John Doe",
    Email: "john@example.com",
    Role:  "Admin",
}

// Insert it into the database
err := client.Insert(ctx, &user)
if err != nil {
    log.Fatalf("Failed to create user: %v", err)
}

// The UID field will be populated after insertion
fmt.Println("Created user with UID:", user.UID)
```

### Upserting Data

dgdao provides a simple API for upserting data into the database.

```go
ctx := context.Background()

user := User{
    Name:  "John Doe", // this field has the `upsert` tag
    Email: "john@example.com",
    Role:  "Admin",
}

// Upsert the user into the database
// If "John Doe" does not exist, it will be created
// If "John Doe" exists, it will be updated
err := client.Upsert(ctx, &user)
if err != nil {
    log.Fatalf("Failed to upsert user: %v", err)
}

```

### Updating Data

To update an existing node, first retrieve it, modify it, then save it back.

```go
ctx := context.Background()

// Get the existing object by UID
var user User
err := client.Get(ctx, &user, "0x1234")
if err != nil {
    log.Fatalf("Failed to get user: %v", err)
}

// Modify fields
user.Name = "Jane Doe"
user.Role = "Manager"

// Save the changes
err = client.Update(ctx, &user)
if err != nil {
    log.Fatalf("Failed to update user: %v", err)
}
```

### Deleting Data

To delete one or more nodes from the database:

```go
ctx := context.Background()

// Delete by UID
err := client.Delete(ctx, []string{"0x1234", "0x5678"})
if err != nil {
    log.Fatalf("Failed to delete node: %v", err)
}
```

### Querying Data

dgdao provides a basic query API for retrieving data:

```go
ctx := context.Background()

// Basic query to get all users
var users []User
err := client.Query(ctx, User{}).Nodes(&users)
if err != nil {
    log.Fatalf("Failed to query users: %v", err)
}

// Query with filters
var adminUsers []User
err = client.Query(ctx, User{}).
    Filter(`eq(role, "Admin")`).
    Nodes(&adminUsers)
if err != nil {
    log.Fatalf("Failed to query admin users: %v", err)
}

// Query with pagination
var pagedUsers []User
err = client.Query(ctx, User{}).
    Filter(`has(name)`).
    Offset(10).
    Limit(5).
    Nodes(&pagedUsers)
if err != nil {
    log.Fatalf("Failed to query paged users: %v", err)
}

// Query with ordering
var sortedUsers []User
err = client.Query(ctx, User{}).
    Order("name").
    Nodes(&sortedUsers)
if err != nil {
    log.Fatalf("Failed to query sorted users: %v", err)
}
```

### Advanced Querying

dgdao is built on top of the [dgman](https://github.com/dolan-in/dgman) package, which provides
access to Dgraph's more powerful and complete query capabilities. For advanced use cases, you can
access the underlying Dgraph client directly and construct more sophisticated queries:

```go
// Define a struct with vector field for similarity search
type Product struct {
    Name        string            `json:"name,omitempty" dgraph:"index=term"`
    Description string            `json:"description,omitempty"`
    Vector      *dgman.VectorFloat32 `json:"vector,omitempty" dgraph:"index=hnsw(metric:cosine)"`

    UID   string   `json:"uid,omitempty"`
    DType []string `json:"dgraph.type,omitempty"`
}

// Get similar products using vector similarity search
func getSimilarProducts(client dg.Client, embeddings []float32) (*Product, error) {
    ctx := context.Background()

    // Convert vector to string format for query
    vectorStr := fmt.Sprintf("%v", embeddings)
    vectorStr = strings.Trim(strings.ReplaceAll(vectorStr, " ", ", "), "[]")

    // Create result variable
    var result Product

    // Get access to the underlying Dgraph client
    dgo, cleanup, err := client.DgraphClient()
    if err != nil {
        return nil, err
    }
    defer cleanup()

    // Construct query using similar_to function with a parameter for the vector
    query := dgman.NewQuery().Model(&result).RootFunc("similar_to(vector, 1, $vec)")

    // Execute query with variables
    tx := dgman.NewReadOnlyTxn(dgo)
    err = tx.Query(query).
        Vars("similar_to($vec: string)", map[string]string{"$vec": vectorStr}).
        Scan()

    if err != nil {
        return nil, err
    }

    return &result, nil
}
```

This example demonstrates vector similarity search for finding semantically similar items - a
powerful feature in Dgraph. You can also access other advanced capabilities like full-text search
with language-specific analyzers, geolocation queries, and more. The ability to access the raw
Dgraph client gives you the full power of Dgraph's query language while still benefiting from
dgdao's simplified client interface and schema management.

## Atomic Operations (`LoadOrStore` and `LoadAndDelete`)

Two key-keyed operations give you atomic insert-if-absent and read-and-consume semantics, named
after their `sync.Map` counterparts. Both key off an upsert predicate — either one you pass
explicitly or the first field tagged `dgraph:"upsert"`.

### LoadOrStore

`LoadOrStore` stores a node only if none already matches the upsert predicate, reporting whether one
already existed. It is the building block for claiming a one-time token: the first caller stores and
proceeds, every later caller sees `loaded == true` and is rejected. On the `loaded == true` path the
passed object is hydrated with the existing record.

```go
type Token struct {
    UID   string   `json:"uid,omitempty"`
    DType []string `json:"dgraph.type,omitempty"`
    JTI   string   `json:"jti,omitempty" dgraph:"index=hash upsert unique"`
}

// false the first time (stored), true thereafter (an existing node matched).
loaded, err := client.LoadOrStore(ctx, &Token{JTI: "abc123"}, "jti")
if err != nil {
    log.Fatalf("LoadOrStore failed: %v", err)
}
```

### LoadAndDelete

`LoadAndDelete` atomically reads a node and deletes it, electing a single winner under concurrency:
exactly one caller gets `loaded == true` with the record hydrated, the rest get `loaded == false`.
The read and delete share one transaction, so concurrent callers conflict on commit — the loser
aborts and retries into not-found. Use it to consume a one-shot value such as a nonce, a pending
job, or a single-use code.

```go
var got Token
loaded, err := client.LoadAndDelete(ctx, &got, "abc123", "jti")
if err != nil {
    log.Fatalf("LoadAndDelete failed: %v", err)
}
if loaded {
    fmt.Println("consumed", got.JTI)
}
```

Both operations are also available on the typed `Client[T]`, returning the record directly rather
than hydrating a passed pointer.

## Deferred-Commit Transactions (`NewTxnContext` and `InTxn`)

`Insert`, `Upsert`, and `Update` each commit as soon as they're called, so none of them can group
several mutations into one atomic write. `NewTxnContext` opens a read-write transaction, and
`client.InTxn(tx)` returns a `Client` whose entire surface — reads, writes, `LoadOrStore`,
`LoadAndDelete` — runs on that transaction. Reads join its read-set, writes stage, and nothing lands
until you call `Commit`.

Two handles cooperate:

- `tx` (the `TxnContext`) carries the transaction's lifecycle (`Commit`, `Discard`), its reads
  (`Query`, `QueryRaw`, `Get`), and its graph-primitive deletes (`DeleteEdge`, `DeleteNode`,
  `DeletePredicate`).
- `client.InTxn(tx)` is the validated CRUD surface scoped to `tx`. Every staged write runs the same
  defaulting, validation, and unique-constraint error translation (to `*UniqueError`) as its
  single-shot counterpart, so grouping mutations costs no safety.

```go
ctx := context.Background()

tx := client.NewTxnContext(ctx)
defer tx.Discard() // no-op after a successful Commit; guarantees cleanup on every other path

sc := client.InTxn(tx) // validated writes and reads scoped to tx

// Remove the asset's edge to its old owner (graph primitive: on tx).
if err := tx.DeleteEdge(asset.UID, "owner", oldOwner.UID); err != nil {
    log.Fatalf("failed to stage edge delete: %v", err)
}

// Remove a stale token node outright.
if err := tx.DeleteNode(staleToken.UID); err != nil {
    log.Fatalf("failed to stage node delete: %v", err)
}

// Stage the new owner. "Jane Doe" has the `upsert` tag, same as the single-shot example above.
if err := sc.Upsert(ctx, &User{Name: "Jane Doe", Role: "Owner"}); err != nil {
    log.Fatalf("failed to stage upsert: %v", err)
}

// Nothing above touched the database until this call.
if err := tx.Commit(); err != nil {
    log.Fatalf("failed to commit transaction: %v", err)
}
```

Nothing reaches the database until `Commit` succeeds; `Discard` abandons every staged mutation
instead. Call `Commit` or `Discard` exactly once. Deferring `Discard` immediately after
`NewTxnContext`, as in the example, is the safe default: it is a no-op once `Commit` has succeeded,
but still returns the pooled connection to the pool on any error or panic path. Schema and lifecycle
operations reached through the scoped client (`UpdateSchema`, `DropAll`, `Close`, ...) are
non-transactional and run on the underlying client unchanged, because Dgraph `Alter` is not part of
a transaction.

### Typed transactions and guarded reads

`typed.Client[T].InTxn(tx)` scopes a typed client to the same transaction, so typed queries and
writes run on `tx` too. This is what makes a guarded read-then-delete correct: a typed `WhereEdge`
query resolves its edge-match pre-pass and its data block against one transactional read-set, so a
concurrent edge change aborts the transaction rather than slipping between the read and the delete.

```go
tx := client.NewTxnContext(ctx)
defer tx.Discard()

owners := typed.NewClient[Owner](client).InTxn(tx)

// Read: find owners whose pet is named "Fido" (the WhereEdge pre-pass runs in tx).
matched, err := owners.Query(ctx).WhereEdge("pets", `eq(name, $1)`, "Fido").Nodes()
if err != nil {
    log.Fatalf("guarded read failed: %v", err)
}

// Delete each matched owner and commit atomically.
for _, o := range matched {
    if err := owners.Delete(ctx, o.UID); err != nil {
        log.Fatalf("failed to stage delete: %v", err)
    }
}
if err := tx.Commit(); err != nil {
    log.Fatalf("commit failed: %v", err) // a conflicting concurrent write aborts here
}
```

> **Backend note.** Reads within a transaction observe writes staged earlier in the same
> transaction (read-your-writes), and transactions abort on conflict, only against a real Dgraph
> cluster. The embedded `file://` engine commits each mutation immediately, so it neither serves
> read-your-writes across an interactive transaction nor aborts on conflict. Reads-only transactions
> and read-then-write-then-commit sequences behave identically on both backends; use a `dgraph://`
> cluster where read-your-writes or conflict-abort semantics matter.

## Retrying Aborted Transactions

Under concurrent load, Dgraph may abort a transaction when two writers touch the same data at once.
The write fails with an aborted-transaction error and is safe to retry: replaying it on a fresh
transaction usually succeeds. `WithRetry` wraps that pattern with exponential backoff, modeled after
dgraph4j's `client.withRetry()`.

You supply a `RetryPolicy` and a function containing the work. `WithRetry` runs the function, and on
an aborted-transaction error waits per the policy's backoff schedule and runs it again, up to
`MaxRetries` additional times. Any other error is returned immediately, and the function always runs
at least once.

```go
ctx := context.Background()

err := client.WithRetry(ctx, dgdao.DefaultRetryPolicy, func() error {
    return client.Insert(ctx, &user)
})
if err != nil {
    log.Fatalf("Insert failed after retries: %v", err)
}
```

`DefaultRetryPolicy` uses 10 retries, a 100ms base delay, a 5s max delay, and 10% jitter. To tune
the schedule, pass your own `RetryPolicy`:

```go
policy := dgdao.RetryPolicy{
    MaxRetries: 5,                      // retry attempts after the initial try
    BaseDelay:  50 * time.Millisecond,  // grows exponentially: BaseDelay * 2^attempt
    MaxDelay:   2 * time.Second,        // caps any single delay
    Jitter:     0.2,                    // random fraction added to spread retriers apart
}

err := client.WithRetry(ctx, policy, func() error {
    return client.Upsert(ctx, &user)
})
```

The context bounds the total wait: if it is cancelled during a backoff sleep, `WithRetry` returns
the context error. Only aborted transactions are retried — a unique-constraint violation, for
example, surfaces to the caller on the first attempt rather than being retried.

## Typed Client (Generic, Type-Safe API)

The `typed` package wraps `dgdao.Client` in a Go generic layer that binds one Go type to the
otherwise `any`-typed client. You get compile-time-typed CRUD and a fluent query builder with no
per-entity code generation. It composes on the same dgman primitives the base client uses, so it
adds type safety without surrendering any of Dgraph's query power.

The base client is value-oriented: its methods take and return `any`, so every call site declares a
destination slice, builds the query, decodes, and re-asserts the type. The typed layer lifts that
repeated shape into the type system once.

```go
import "github.com/dgraph-io/dgdao/typed"

// Bind the client to User once.
users := typed.NewClient[User](client)

// Nodes returns []User directly — no destination slice, no decode step.
admins, err := users.Query(ctx).
    Filter(`eq(role, $1)`, "Admin").
    OrderAsc("name").
    Limit(50).
    Nodes()

// First returns *User, and nil when nothing matched.
admin, err := users.Query(ctx).
    Filter(`eq(role, $1)`, "Admin").
    First()
if err != nil {
    log.Fatalf("query admin: %v", err)
}
if admin == nil {
    log.Fatal("no admin matched") // First returns nil when nothing matched
}

// Add, Update, and Upsert take *User; Get and Delete are keyed by a UID string (Get returns *User).
got, err := users.Get(ctx, admin.UID)
```

### Query builder

`Query[T]` chains builder methods and ends in a terminal that executes and decodes a typed result:
`Nodes()` returns `[]T`, `First()` returns `*T`, `NodesAndCount()` returns `[]T` plus the total
count, and `IterNodes()` returns an iterator of `*T`.

- **Filters** accumulate and AND together. Each fragment is parenthesized, so a fragment containing
  `OR` keeps its precedence when combined.
- **`OrGroup`** ORs several sub-scopes into one parenthesized group:

  ```go
  // role == "Admin" AND (name == "Alice" OR name == "Bob")
  users.Query(ctx).
      Filter(`eq(role, $1)`, "Admin").
      OrGroup(
          typed.NewDetachedQuery[User]().Filter(`eq(name, "Alice")`),
          typed.NewDetachedQuery[User]().Filter(`eq(name, "Bob")`),
      ).Nodes()
  ```

- **`WhereEdge`** constrains `T` by a scalar on a neighbour reached over an edge, which a root
  filter cannot express. It renders a server-side `var` block, so the matched UIDs never leave the
  server and memory stays bounded no matter how many roots match. When you also set a root, the edge
  match intersects it rather than replacing it.
- **`IterNodes`** streams arbitrarily large result sets one page at a time over a single read-only
  snapshot.
- **`MultiQuery`** batches several same-type blocks into one round-trip:

  ```go
  // One request, results keyed by block name: map[string][]User.
  results, err := typed.NewMultiQuery[User](client).
      Add("admins", users.Query(ctx).Filter(`eq(role, $1)`, "Admin")).
      Add("guests", users.Query(ctx).Filter(`eq(role, $1)`, "Guest")).
      Execute(ctx)
  ```

The companion `typed/filter` and `typed/search` packages add a parameterised filter-expression
builder and helpers for merging ranked results across blocks.

For the full API, the design rationale, and runnable examples, see the package documentation
(`go doc github.com/dgraph-io/dgdao/typed`) and the `example_test.go` files under `typed/`.

## Automatic Similarity Search (`SimString`)

`SimString` is a string type that transparently manages vector embeddings and HNSW-indexed shadow
predicates. When a struct field of this type is tagged with `dgraph:"embedding"`, dgdao
automatically calls the configured `EmbeddingProvider` on every insert, upsert, and update, storing
the resulting vector in a `<fieldname>__vec` shadow predicate. This eliminates the need to manually
maintain `VectorFloat32` fields or call embedding APIs.

### Setup

Configure an embedding provider when creating the client:

```go
import dg "github.com/dgraph-io/dgdao"

// OpenAICompatibleProvider works with OpenAI, Ollama, and any OpenAI-compatible endpoint.
provider := dg.NewOpenAICompatibleProvider(dg.OpenAICompatibleConfig{
    BaseURL: "http://localhost:11434", // Ollama; use "https://api.openai.com" for OpenAI
    Model:   "bge-m3:latest",
    Dims:    1024,
    // APIKey: os.Getenv("OPENAI_API_KEY"), // required for OpenAI
})

client, err := dg.NewClient(uri,
    dg.WithAutoSchema(true),
    dg.WithEmbeddingProvider(provider),
)
```

### Defining a struct with `SimString`

```go
type Product struct {
    Name        string       `json:"name,omitempty"        dgraph:"index=term"`
    // index=term — also maintain a standard term index on the text predicate
    // embedding  — auto-embed on every write
    // threshold=20 — skip embedding (and delete stale vector) for very short strings
    Description dg.SimString `json:"description,omitempty" dgraph:"index=term,embedding,threshold=20"`

    UID   string   `json:"uid,omitempty"`
    DType []string `json:"dgraph.type,omitempty"`
}
```

When `AutoSchema` is enabled, `UpdateSchema` automatically registers the shadow predicate:

```go
description__vec: float32vector @index(hnsw(exponent: "4", metric: "cosine")) .
```

### Inserting and updating

No changes to the regular insert/update API — the embedding happens automatically:

```go
ctx := context.Background()

product := &Product{
    Name:        "Trail Runner X",
    Description: "Lightweight trail running shoe with aggressive grip for mountain terrain",
}
err := client.Insert(ctx, product)
// product.UID is now set; description__vec has been written automatically.

// Update: the shadow vector is re-embedded along with the text change.
product.Description = "Waterproof trail shoe with rock plate for muddy mountain terrain"
err = client.Update(ctx, product)
```

### Querying by similarity

Use `SimilarToText` to embed a query string and find the nearest neighbours in a single call:

```go
var result Product
err := dg.SimilarToText(client, ctx, &result, "description", "running shoes for trails", 1)
if err != nil {
    log.Fatal(err)
}
fmt.Println("Best match:", result.Name)
```

For queries where you already have a pre-computed vector, use `SimilarTo` with an explicit
`*dgman.TxnContext`:

```go
dgoClient, cleanup, err := client.DgraphClient()
defer cleanup()

tx := dgman.NewReadOnlyTxn(dgoClient)

var result Product
err = dg.SimilarTo(tx, &result, "description", myVec, 5).Scan()
```

### `embedding` tag options

| Option      | Default  | Description                                                                                |
| ----------- | -------- | ------------------------------------------------------------------------------------------ |
| `metric`    | `cosine` | HNSW distance metric: `cosine`, `euclidean`, or `dotproduct`                               |
| `exponent`  | `4`      | HNSW index size exponent                                                                   |
| `threshold` | `0`      | Minimum rune count to embed. Below this, the shadow vector is **deleted** (not left stale) |

You can combine `embedding` with any standard string index, e.g. `dgraph:"embedding,index=term"` to
enable both term search and similarity search on the same predicate.

### Implementing a custom provider

Any type that satisfies `EmbeddingProvider` can be used:

```go
type EmbeddingProvider interface {
    Embed(ctx context.Context, text string) ([]float32, error)
    Dims() int
}
```

## Schema Management

dgdao provides robust schema management features that simplify working with Dgraph's schema system.

### AutoSchema

The AutoSchema feature automatically generates and updates the database schema based on your Go
struct definitions. When enabled, dgdao will analyze the struct tags of objects you insert and
ensure the appropriate schema exists in the database.

Enable AutoSchema when creating a client:

```go
// Enable automatic schema management
client, err := dg.NewClient(uri, dg.WithAutoSchema(true))
if err != nil {
    log.Fatalf("Failed to create client: %v", err)
}

// Now you can insert objects without manually creating the schema first
user := User{
    Name:  "John Doe",
    Email: "john@example.com",
}

// The schema will be automatically created or updated as needed
err = client.Insert(ctx, &user)
```

With AutoSchema enabled, dgdao will:

1. Analyze the struct tags of objects being inserted
2. Generate the appropriate Dgraph schema based on these tags
3. Apply any necessary schema updates to the database
4. Handle type definitions for node types based on struct names

This is particularly useful during development when your schema is evolving frequently.

Special note regarding changing/deleting fields: removing a field from a struct WILL NOT remove the
field and any associated data from the database. See the `TestDeletePredicate` in `delete_test.go`
for an example of how to delete a predicate(field) from all nodes that have it. Similarly, changing
the type of a field will not convert existing data to the new type.

### Schema Operations

For more control over schema management, dgdao provides several methods in the Client interface:

#### UpdateSchema

Manually update the schema based on one or more struct types:

```go
// Update schema based on User and Post structs
err := client.UpdateSchema(ctx, User{}, Post{})
if err != nil {
    log.Fatalf("Failed to update schema: %v", err)
}
```

This is useful when you want to ensure the schema is created before inserting data, or when you need
to update the schema for new struct types.

#### AlterSchema

`UpdateSchema` infers the schema from Go struct tags, which is convenient but cannot express
predicates no Go type models yet. `AlterSchema` is the escape hatch: it applies a raw Dgraph Schema
Definition Language string directly, giving full control over predicate types, indexes, and
directives. This is the common case during a migration that adds or reshapes predicates ahead of the
code.

```go
// Apply raw DQL schema: predicate types, indexes, and directives.
err := client.AlterSchema(ctx, `
    name:  string @index(exact) .
    email: string @index(hash) @upsert .
    age:   int    @index(int) .
`)
if err != nil {
    log.Fatalf("Failed to alter schema: %v", err)
}
```

`AlterSchema` behaves the same for embedded (`file://`) and remote (`dgraph://`) clients. To drop a
single predicate and its data, issue an `Alter` with `DropAttr` set through the underlying Dgraph
client (see [`DgraphClient`](client.go)) — that path now behaves identically in both modes.

#### GetSchema

Retrieve the current schema definition from the database:

```go
// Get the current schema
schema, err := client.GetSchema(ctx)
if err != nil {
    log.Fatalf("Failed to get schema: %v", err)
}

fmt.Println("Current schema:")
fmt.Println(schema)
```

The returned schema is in Dgraph Schema Definition Language format.

#### DropAll and DropData

Reset the database completely or just clear the data:

```go
// Remove all data but keep the schema
err := client.DropData(ctx)
if err != nil {
    log.Fatalf("Failed to drop data: %v", err)
}

// Or remove both schema and data
err = client.DropAll(ctx)
if err != nil {
    log.Fatalf("Failed to drop all: %v", err)
}
```

These operations are useful for testing or when you need to reset your database state.

## Limitations

dgdao has a few limitations to be aware of:

- **Schema evolution**: While dgdao supports schema inference through tags, evolving an existing
  schema with new fields requires careful consideration to avoid data inconsistencies.

## CLI Commands and Examples

dgdao provides several command-line tools and example applications to help you interact with and
explore the package. These are organized in the `cmd` and `examples` folders:

### Commands (`cmd` folder)

- **`cmd/query`**: A flexible CLI tool for running arbitrary DQL (Dgraph Query Language) queries
  against a dgdao database.
  - Reads a query from standard input and prints JSON results.
  - Supports file-based dgdao storage.
  - Flags: `--dir`, `--pretty`, `--timeout`, `-v` (verbosity).
  - See [`cmd/query/README.md`](./cmd/query/README.md) for usage and examples.

### Examples (`examples` folder)

- **`examples/basic`**: Demonstrates CRUD operations for a simple `Thread` entity.

  - Flags: `--dir`, `--addr`, `--cmd`, `--author`, `--name`, `--uid`, `--workspace`.
  - Supports create, update, delete, get, and list commands.
  - See [`examples/basic/README.md`](./examples/basic/README.md) for details.

- **`examples/load`**: Shows how to load the standard 1million RDF dataset into dgdao for
  benchmarking.

  - Downloads, initializes, and loads the dataset into a specified directory.
  - Flags: `--dir`, `--verbosity`.
  - See [`examples/load/README.md`](./examples/load/README.md) for instructions.

You can use these tools as starting points for your own applications or as references for
integrating dgdao into your workflow.

## Extensions

dgdao has companion projects under the `dgraph-io` organization that extend it with code generation,
schema migrations, and observability. Each is an independent Go module you can adopt on its own.

### dgdao-gen

Code generator and wrapper-entity runtime. Define your Go structs, run `go generate`, and get a
fully typed client, query builders, and auto-paging iterators derived from your schema. See
[github.com/dgraph-io/dgdao-gen](https://github.com/dgraph-io/dgdao-gen).

```sh
go install github.com/dgraph-io/dgdao-gen/cmd/dgdao-gen@latest
```

Generated code imports `github.com/dgraph-io/dgdao-gen/wrap`.

### dgdao-migrate

Struct-first schema migration framework for Dgraph. Manages your schema lifecycle as an ordered
chain of run-once migrations (schema and data changes) scaffolded from your Go struct snapshots,
with an explicit revision chain, resumable phased migrations, checksum-enforced immutability, and
drift gates. See [github.com/dgraph-io/dgdao-migrate](https://github.com/dgraph-io/dgdao-migrate).

```go
import "github.com/dgraph-io/dgdao-migrate/migrate"
```

### dgdao-telemetry

OpenTelemetry instrumentation for the dgdao typed client. Provides the OpenTelemetry implementation
of the pluggable `typed.Tracer`, emitting a client span per database operation. See
[github.com/dgraph-io/dgdao-telemetry](https://github.com/dgraph-io/dgdao-telemetry).

```go
import telemetry "github.com/dgraph-io/dgdao-telemetry"
```

## Open Source

We welcome external contributions. See the [CONTRIBUTING.md](./CONTRIBUTING.md) file if you would
like to get involved.

dgdao and its components are © Istari Digital, Inc., and licensed under the terms of the Apache
License, Version 2.0. See the [LICENSE](./LICENSE) file for a complete copy of the license.

## Windows Users

dgdao (and its dependencies) are designed to work on POSIX-compliant operating systems, and are not
guaranteed to work on Windows.

Tests at the top level folder (`go test .`) on Windows are maintained to pass on Windows, but other
tests in subfolders may not work as expected.

Temporary folders created during tests may not be cleaned up properly on Windows. Users should
periodically clean up these folders. The temporary folders are created in the Windows temp
directory, `C:\Users\<username>\AppData\Local\Temp\dgdao_test*`.

## Contributing

See the [CONTRIBUTING.md](./CONTRIBUTING.md) file for information on how to contribute to dgdao.

## Acknowledgements

dgdao builds heavily upon packages from the open source projects of
[Dgraph](https://github.com/dgraph-io/dgraph) (graph query processing and transaction management),
[Badger](https://github.com/dgraph-io/badger) (data storage), and
[Ristretto](https://github.com/dgraph-io/ristretto) (cache). dgdao also relies on the
[dgman](https://github.com/dolan-in/dgman) repository for much of its functionality. We expect the
architecture and implementations of dgdao and Dgraph to expand in differentiation over time as the
projects optimize for different core use cases, while maintaining Dgraph Query Language (DQL)
compatibility.
