---
date: 2026-05-21
topic: query-edge-filter
status: implemented
---

# Edge-Predicate Filtering for Generated Query Builders

## Goal

Let a generated `<Entity>Query` filter root records by a scalar predicate of a _neighbouring_ node
reached over an edge — "people who have a dog named Fido" — as a first-class, generated method:

```go
client.Person.Query(ctx).WhereDogs(`eq(name, "Fido")`).Nodes()
```

Today `<Entity>Query.Filter` (and the `typed.Query[T].Filter` it delegates to) only constrains the
root node's _own_ predicates: the filter string lands in dgraph's root `@filter`, which has no
syntax for an edge target's scalar value. There is no way, short of hand-written DQL through
`Client.QueryRaw`, to express "root has an edge whose target matches X."

## Non-Goals

- **A typed predicate DSL.** `WhereDogs(filter string, params ...any)` takes a dgraph `@filter`
  string, exactly like the existing `Filter`. A type-safe
  `WhereDogs(func(c *DogCriteria){ c.NameEq("Fido") })` face is future work; it would layer over the
  same `WhereEdge` substrate this spec introduces.
- **Multi-hop filters** (root → edge → edge). The filter string constrains the _immediate_ edge
  target's own predicates.
- **Changing `Filter`, `Nodes`, `First`, `IterNodes`, or CRUD.**

## Why This Approach

**dgman emits one query block.** A `typed.Query[T]` wraps a single `*dg.Query`, which dgman renders
as one root `@filter` over an `expand` body (`query.go:generateQuery`). dgman exposes no way to
attach a `@filter` to an edge sub-block. So edge filtering cannot be a new dgman builder call — it
needs a genuinely separate execution path.

**Server-side semi-join in one request.** A query carrying edge constraints runs as a single
multi-block DQL request:

1. **Var block** —
   `<var> as var(func: <root>) @cascade { … one filtered block per edge constraint }`. `@cascade`
   drops any node with an empty block, so the `as` variable binds exactly the roots that satisfy
   every constraint. The variable lives on the server.
2. **Data block** — the existing `*dg.Query`, intersected with `uid(<var>)`, carrying the caller's
   `Filter`, ordering, pagination, and dgman's normal projection.
3. **Count block** (only for `NodesAndCount`) — `count(uid)` over `uid(<var>)` with the caller's
   `Filter` re-applied, so the total matches the rows the data block would return without
   pagination.

The matched UIDs are never returned to the client or inlined into a `uid(0x1, 0x2, …)` literal.
Memory and DQL size stay bounded by the query, not by how many roots match. This is the same shape
dgman's own `NodesAndCount` uses internally (`filtered as var(...)` feeding `func: uid(filtered)`).

**On the rejected alternative.** An earlier draft of this design ran the semi-join client-side — a
pre-pass returned the matching UIDs, which the main query then inlined as `uid(<matched>)` — and
rejected a `QueryRaw` two-block query on the grounds that it would force re-implementing the result
projection (`expand` drops managed reverse edges, `reverse_test.go`). That reasoning assumed the
data block's body would be _hand-written_. It need not be: rendering the request with
`dg.NewQueryBlock(varBlock, dataBlock).String()` lets dgman generate the data block's projection
exactly as it does for a normal query, so reverse-edge-aware expansion is preserved. The request is
then executed with `Client.QueryRaw`, and the data block is decoded through the typed
predicate-remap path (see `multi_query.go`). This is strictly better than the client-side pre-pass:
the UIDs stay on the server (bounded memory and DQL size), and for the single-shot terminals
(`Nodes`, `First`, `NodesAndCount`) the whole semi-join runs in one read-only transaction, closing
the second-read consistency window the client-side form had.

**`IterNodes`.** Each page is its own request that re-resolves the var block server-side and pages
the data block with `first`/`offset`. Memory stays bounded regardless of result size — the property
the streaming terminal advertises — at the cost of re-running the `@cascade` match per page, and of
reading each page from a fresh snapshot rather than one transaction. For the unbounded-result case
this terminal exists to serve, bounded memory is the property that matters.

## Design

### `typed.Query[T]` — the `WhereEdge` substrate

`Query[T]` carries `conn`/`ctx` (to run the request) and an `edges` slice; `customRootExpr` records
a caller's `UID`/`RootFunc` narrowing so the var block can root at it:

```go
type Query[T any] struct {
    q              *dg.Query
    conn           dgdao.Client
    ctx            context.Context
    limit          int
    offset         int
    edges          []edgeFilter
    filters        []filterFrag
    customRootExpr string
}

type edgeFilter struct {
    predicate string
    filter    string
    params    []any
}
```

New builder, accumulating (each call ANDs another constraint):

```go
func (qb *Query[T]) WhereEdge(predicate, filter string, params ...any) *Query[T]
```

The terminals (`Nodes`, `First`, `IterNodes`, `NodesAndCount`) check for edge constraints. With
none, they run the plain dgman query unchanged. With constraints, they call `runEdge`, which
assembles the var/data/(count) blocks, renders them with `dg.NewQueryBlock`, runs the request via
`QueryRaw`, and decodes. `runEdge` pushes the data-block filter onto `qb.q` last-write-wins and
never mutates the accumulated filters, so `IterNodes` can call it once per page.

### Server-side var DQL

For `WhereEdge("pets", "eq(name, $1)", "Fido")` over `Owner`, with a root filter
`Filter("eq(name, \"Alice\")")` and `NodesAndCount`:

```dql
{
  mgMatched as var(func: type(Owner)) @filter(has(dgraph.type)) @cascade {
    uid
    mg_e0 : pets @filter(eq(name, "Fido")) { uid }
  }
  mgData(func: type(Owner)) @filter(has(dgraph.type) AND (eq(name, "Alice")) AND uid(mgMatched)) {
    uid
    expand(_all_) { … }
  }
  mgCount(func: uid(mgMatched)) @filter(has(dgraph.type) AND (eq(name, "Alice"))) {
    count(uid)
  }
}
```

The var block is built by reconfiguring a fresh `conn.Query(ctx, &T{})` with
`As(mgMatched).Var().Cascade().Query(body, params...)`; when the caller narrowed the root, the var
block roots at `customRootExpr` so the match is the intersection of the caller's root and the edge
constraints, not an overwrite. Every edge block is aliased `mg_e0`, `mg_e1`, … so two constraints on
the same predicate do not collide as duplicate fields. Each edge filter is written numbering its
params from `$1`; `shiftPlaceholders` renumbers them against the concatenated params slice before
they are joined into one body.

### Generated face — `<Entity>Query.Where<Edge>`

`wrapper_query.go.tmpl` emits one thin method per edge field, delegating to the substrate — the same
pattern `Filter`/`Cascade` already use:

```go
func (q *OwnerQuery) WherePets(filter string, params ...any) *OwnerQuery {
    q.typed.WhereEdge("pets", filter, params...)
    return q
}
```

The method name is `Where` + the field's accessor name; the predicate string is the field's resolved
dgraph predicate. Generated for every edge field (multi, singular, and reverse). No parser changes —
`model.Field` already carries `IsEdge`/`Predicate`.

## Error handling

`WhereEdge` never executes — it only appends. The request error (malformed filter, transport
failure) surfaces from the terminal: `Nodes`/`First`/ `NodesAndCount` return it; `IterNodes` yields
one `(nil, err)` and stops. A var block matching zero roots is not an error — `uid()` of an empty
var yields no rows, so the terminal returns an empty result.

## Testing

- **`typed/query_test.go`** — `owner`/`pet` test types (an edge pair). Behavioral tests against the
  file engine: `WhereEdge` filters by edge target; no match yields empty; `$N` params bind;
  `WhereEdge` composes with a root `Filter`; a `UID` root is preserved (intersection, not
  overwrite); two `WhereEdge` calls AND; `First`, `IterNodes`, and `NodesAndCount` honor edge
  constraints (the count reflects the full match, independent of `Limit`).
- **`typed/query_internal_test.go`** — white-box assertion that the rendered DQL is a server-side
  var (`mgMatched as var(`, `uid(mgMatched)`) carrying no inlined `uid(0x…)` literal list.
- **`generator_test.go`** — a two-type edge schema asserts `Where<Edge>` is generated and delegates
  to `typed.WhereEdge`, and that an edgeless type gets no `Where*` method.
- **`wrapper_query_e2e_test.go`** — `client.Director.Query(ctx).WhereFilms(...)` end-to-end against
  the file-backed client.

## Migration / blast radius

- **Modified:** `typed/query.go` (struct fields, `WhereEdge`, the
  `runEdge`/`edgeBlocks`/`edgeVarBlock`/`edgeCountBlock`/`edgeMatchBody`/ `shiftPlaceholders`
  helpers, edge-aware terminals, doc comments); `typed/client.go` (`Query` passes `conn`/`ctx`);
  `wrapper_query.go.tmpl` (generated `Where<Edge>`).
- **Regenerated:** the `movies` fixture — every `*_query_gen.go` for an entity with edges gains
  `Where<Edge>` methods.
- **New tests** in `typed/query_test.go`, `typed/query_internal_test.go`, `generator_test.go`,
  `wrapper_query_e2e_test.go`.
- No change to `Filter`, `Nodes`, `First`, `IterNodes`, CRUD, or any other generated artifact. The
  var-block path is inert unless `WhereEdge` is called.

## Open decisions

None. The string-filter API (over a typed DSL) and one-hop depth were settled before implementation;
the typed predicate DSL is recorded above as future work. The semi-join runs server-side via a var
block rather than the client-side pre-pass an earlier draft proposed, so matched UIDs stay off the
client — see _Why This Approach_.
