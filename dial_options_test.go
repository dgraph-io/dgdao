/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"testing"

	"google.golang.org/grpc"
)

func TestWithGRPCDialOptionAppends(t *testing.T) {
	var o clientOptions
	WithGRPCDialOption(grpc.WithUserAgent("a"))(&o)
	WithGRPCDialOption(grpc.WithUserAgent("b"))(&o)
	if got := len(o.grpcDialOptions); got != 2 {
		t.Fatalf("expected 2 dial options, got %d", got)
	}
}

func TestKeyDistinguishesGRPCDialOptions(t *testing.T) {
	// A client with no dial options must differ from one with a dial option.
	base := client{uri: "dgraph://localhost:9080"}
	withOpt := client{uri: "dgraph://localhost:9080"}
	WithGRPCDialOption(grpc.WithUserAgent("x"))(&withOpt.options)
	if base.key() == withOpt.key() {
		t.Fatal("client.key() must differ when grpcDialOptions differ, else clients dedup incorrectly")
	}

	// Two clients with the SAME number of dial options but DIFFERENT options
	// must also differ. A count-only key would collide here and merge clients
	// that were configured differently.
	a := client{uri: "dgraph://localhost:9080"}
	b := client{uri: "dgraph://localhost:9080"}
	WithGRPCDialOption(grpc.WithUserAgent("x"))(&a.options)
	WithGRPCDialOption(grpc.WithUserAgent("y"))(&b.options)
	if a.key() == b.key() {
		t.Fatal("client.key() must differ when dial options differ at the same count")
	}
}

func TestKeyIgnoresGRPCDialOptionsForEmbedded(t *testing.T) {
	// Dial options are ignored for embedded (file://) URIs, so they must not
	// affect the dedup key there.
	plain := client{uri: "file:///tmp/db"}
	withOpt := client{uri: "file:///tmp/db"}
	WithGRPCDialOption(grpc.WithUserAgent("x"))(&withOpt.options)
	if plain.key() != withOpt.key() {
		t.Fatal("dial options must not affect the cache key for embedded (file://) clients")
	}
}
