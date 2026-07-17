# Changelog

## 2026-07-17 - Version 0.7.0

- feat: add `Client.NewTxnContext`, a validated, deferred-commit read-write transaction for
  multi-mutation atomic writes (`Upsert`/`Update`/`Insert`/`DeleteEdge`/`DeleteNode`/
  `DeletePredicate`), with the same validation and unique-error handling as the single-shot methods

## 2026-07-10 - Version 0.6.0

- feat: add the `Defaulter` interface; dgdao calls `ApplyDefaults(ctx)` on the model before
  validation in `Insert`, `Upsert`, `Update`, and `LoadOrStore` (not `LoadAndDelete`), so a model
  can populate default field values — and a defaulted field can satisfy a `validate:"required"`
  rule. Applies across slices for batch writes. See `DEFAULTER.md`.

## 2026-07-09 - Version 0.5.4

- chore(deps): pin dgraph/v25 to the released v25.3.8 tag, replacing a pre-release pseudo-version
- chore(deps): update badger to v4.9.4 and ristretto to v2.4.2, both required by dgraph v25.3.8
- docs: add an Extensions section covering dgdao-gen, dgdao-migrate, and dgdao-telemetry

## 2026-07-07 - Version 0.5.3

- breaking: rename the module from `github.com/matthewmcneely/modusgraph` to
  `github.com/dgraph-io/dgdao`; importers must update their import paths
- feat: add a generic, type-safe client and query builder
- feat: add `Client.LoadOrStore` and `Client.LoadAndDelete`, with typed `Client[T]` equivalents
- feat: add `SelfValidator` for custom and cross-field validation
- feat: add `AlterSchema`, `dropPredicate`, and embedded `DropAttr`
- feat: add `WithGRPCDialOption` for custom gRPC dial settings
- feat: add `WithRetry` and a configurable aborted-transaction retry policy
- feat: recognize generated schema types via `SchemaTypeName` and `UnwrapSchema`
- fix: resolve the Dgraph type name from the `DType` tag during mutation validation
- fix: key the client dedup cache on dial-option identity rather than option count
- fix: forward GraphQL variables through the `WhereEdge` var-block path
- fix: correct filter precedence and preserve root nodes under `WhereEdge`
- chore: upgrade Dgraph to main HEAD following the hooks merge
- ci: adopt the Go 1.26 toolchain that Dgraph requires

## 2026-05-27 - Version 0.5.2

- feat: add `WithMaxRecvMsgSize` to raise the gRPC receive limit

## 2026-05-27 - Version 0.5.1

- fix: correct partial updates on `SimString` fields

## 2026-02-18 - Version 0.5.0

- breaking: `SimilarToText` now executes the query internally and returns only `error`, replacing
  `(*dg.QueryBlock, error)`
- feat: add automatic embedding-backed similarity search
- feat: add an embedding provider keyed on the client map hash
- fix: discard each transaction from a single deferred call, releasing connections promptly
- chore: track a supporting release of dgman and drop the local replace directive
- ci: run the suite against a remote Dgraph cluster

## 2026-02-12 - Version 0.4.0

- breaking: migrate to the new embedded Dgraph client
- feat: add validation support
- fix: correct engine lifecycle handling
- fix: enforce namespace isolation
- chore: update the Dgraph dependency

## 2026-01-23 - Version 0.3.2

- feat: add managed reverse edges and validator support
- fix: guard against nil pointers
- chore: update the Go toolchain and dependencies

## 2025-10-20 - Version 0.3.1

- chore: update to Dgraph v25.0.0 and dgo v250.0.0

## 2025-10-15 - Version 0.3.0

- feat: add new InsertRaw function
- chore: add throughput tests

## 2025-07-22 - Version 0.2.0

- feat: introduce new API that works with local mode and remote clusters
- chore: remove deprecated API

## 2025-05-21 - Version 0.1.0

Baseline for the changelog.

See git commit history for changes for this version and prior.
