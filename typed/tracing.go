/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package typed

import (
	"context"
	"reflect"
	"sync/atomic"
)

// Span is a tracing span for a single database operation. End is called once,
// with the operation's final error (nil on success).
type Span interface {
	End(err error)
}

// Tracer starts a Span around a typed-layer database operation. The typed
// client calls the installed Tracer for every DB call; the default is a no-op,
// so the typed package itself carries no tracing dependency. Install a real
// tracer — for example github.com/mlwelles/dgdao-telemetry's OpenTelemetry
// tracer — with SetTracer.
type Tracer interface {
	// StartSpan begins a span for operation op (for example "get") on the named
	// collection, returning a context carrying the span and the Span itself.
	StartSpan(ctx context.Context, op, collection string) (context.Context, Span)
}

type noopSpan struct{}

func (noopSpan) End(error) {}

type noopTracer struct{}

func (noopTracer) StartSpan(ctx context.Context, _, _ string) (context.Context, Span) {
	return ctx, noopSpan{}
}

// tracerHolder is the process-wide tracer the typed package uses, held in an
// atomic.Pointer so SetTracer and the per-operation reads in currentTracer are
// data-race free. It is a no-op until a host installs one via SetTracer.
var tracerHolder atomic.Pointer[Tracer]

// currentTracer returns the installed tracer, or the no-op tracer if SetTracer
// has not run. Every terminal reads through here, so the load stays on the hot
// path; atomic.Pointer makes it lock-free.
func currentTracer() Tracer {
	if p := tracerHolder.Load(); p != nil {
		return *p
	}
	return noopTracer{}
}

// SetTracer installs the process-wide tracer for typed-layer DB spans. Passing
// nil restores the no-op tracer. It is safe to call concurrently with active
// queries: in-flight terminals keep the tracer they already loaded, and later
// terminals observe the new one.
func SetTracer(t Tracer) {
	if t == nil {
		t = noopTracer{}
	}
	tracerHolder.Store(&t)
}

// entityName returns the unqualified Go type name of T (for example "Resource"),
// used as the db.collection.name span attribute.
func entityName[T any]() string {
	return reflect.TypeFor[T]().Name()
}
