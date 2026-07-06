/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"regexp"
	"strings"

	dg "github.com/dolan-in/dgman/v2"
)

// UniqueError represents an error that occurs when attempting to insert or update
// a node that would violate a unique constraint.
type UniqueError = dg.UniqueError

// parseUniqueError attempts to parse a Dgraph unique constraint violation error
// and convert it to a UniqueError. Returns nil if the error is not a unique constraint violation.
func parseUniqueError(err error) *UniqueError {
	if err == nil {
		return nil
	}

	errStr := err.Error()

	// Match Dgraph unique constraint error format:
	// "could not insert duplicate value [Value] for predicate [predicate]"
	re := regexp.MustCompile(`could not insert duplicate value \[([^\]]+)\] for predicate \[([^\]]+)\]`)
	matches := re.FindStringSubmatch(errStr)
	if len(matches) == 3 {
		return &UniqueError{
			Field: matches[2],
			Value: matches[1],
		}
	}

	// Also check for dgman's error format:
	// " with field=value already exists at uid=0x..."
	if strings.Contains(errStr, "already exists at uid=") {
		re2 := regexp.MustCompile(`with ([^=]+)=([^ ]+) already exists at uid=([^ ]+)`)
		matches2 := re2.FindStringSubmatch(errStr)
		if len(matches2) == 4 {
			return &UniqueError{
				Field: matches2[1],
				Value: matches2[2],
				UID:   matches2[3],
			}
		}
	}

	return nil
}
