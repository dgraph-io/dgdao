/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"time"

	dg "github.com/dgraph-io/dgdao"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ExampleWithGRPCDialOption configures gRPC dial settings the dedicated options
// do not cover — here, transport credentials and keepalive parameters — when
// opening a remote dgraph:// connection. Each WithGRPCDialOption adds one
// grpc.DialOption; they compose with WithMaxRecvMsgSize. The options are ignored
// for embedded (file://) URIs.
func ExampleWithGRPCDialOption() {
	client, err := dg.NewClient(
		"dgraph://localhost:9080",
		dg.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		dg.WithGRPCDialOption(grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		})),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()
}
