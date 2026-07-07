/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// recordingValidator counts StructCtx calls so tests can assert which path ran.
type recordingValidator struct{ calls int }

func (r *recordingValidator) StructCtx(_ context.Context, _ interface{}) error {
	r.calls++
	return nil
}

var errSelfValidated = errors.New("self-validated")

type selfValidatingEntity struct{ Name string }

func (s *selfValidatingEntity) ValidateWith(_ context.Context, _ StructValidator) error {
	return errSelfValidated
}

type plainEntity struct{ Name string }

func TestValidateRoutesToSelfValidator(t *testing.T) {
	rv := &recordingValidator{}
	c := client{options: clientOptions{validator: rv}}

	err := c.validateStruct(context.Background(), &selfValidatingEntity{Name: "x"})
	if !errors.Is(err, errSelfValidated) {
		t.Fatalf("expected the SelfValidator path, got %v", err)
	}
	if rv.calls != 0 {
		t.Fatalf("StructCtx must not run for a SelfValidator, got %d calls", rv.calls)
	}
}

func TestValidateFallsBackToStructCtx(t *testing.T) {
	rv := &recordingValidator{}
	c := client{options: clientOptions{validator: rv}}

	if err := c.validateStruct(context.Background(), &plainEntity{Name: "x"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rv.calls != 1 {
		t.Fatalf("expected StructCtx to run once, got %d", rv.calls)
	}
}

func TestValidateSelfValidatorInSlice(t *testing.T) {
	rv := &recordingValidator{}
	c := client{options: clientOptions{validator: rv}}

	err := c.validateStruct(context.Background(), []*selfValidatingEntity{{Name: "a"}})
	if !errors.Is(err, errSelfValidated) {
		t.Fatalf("expected the SelfValidator path for slice elements, got %v", err)
	}
	// As in the scalar case, the SelfValidator path must not also invoke the
	// configured StructValidator for slice elements.
	if rv.calls != 0 {
		t.Fatalf("StructCtx must not run for a SelfValidator slice element, got %d calls", rv.calls)
	}
}

// dateRange validates a relationship between two fields — a cross-field rule
// that struct tags alone cannot express.
type dateRange struct {
	Start int
	End   int
}

func (d *dateRange) ValidateWith(_ context.Context, _ StructValidator) error {
	if d.End < d.Start {
		return fmt.Errorf("End (%d) must be >= Start (%d)", d.End, d.Start)
	}
	return nil
}

func TestSelfValidatorCustomCrossFieldRule(t *testing.T) {
	c := client{options: clientOptions{validator: &recordingValidator{}}}

	if err := c.validateStruct(context.Background(), &dateRange{Start: 1, End: 5}); err != nil {
		t.Fatalf("a valid range should pass the cross-field rule: %v", err)
	}
	if err := c.validateStruct(context.Background(), &dateRange{Start: 5, End: 1}); err == nil {
		t.Fatal("End < Start must fail the custom cross-field rule")
	}
}

// The tests below exercise the DEFAULT client — no StructValidator configured
// (validator == nil). Before the nil check was relocated out of validateStruct
// into validateOne's StructValidator fallback, validateStruct returned early
// for this client, so SelfValidator never ran: the feature was dead code
// exactly for the common default case. These would fail against that ordering.

func TestValidateSelfValidatorRunsWithoutStructValidator(t *testing.T) {
	c := client{options: clientOptions{validator: nil}}

	err := c.validateStruct(context.Background(), &selfValidatingEntity{Name: "x"})
	if !errors.Is(err, errSelfValidated) {
		t.Fatalf("SelfValidator must run on the default client (no StructValidator), got %v", err)
	}
}

func TestValidateSelfValidatorSliceRunsWithoutStructValidator(t *testing.T) {
	c := client{options: clientOptions{validator: nil}}

	err := c.validateStruct(context.Background(), []*selfValidatingEntity{{Name: "a"}})
	if !errors.Is(err, errSelfValidated) {
		t.Fatalf("SelfValidator must run for slice elements on the default client, got %v", err)
	}
}

func TestValidatePlainEntitySkippedWithoutStructValidator(t *testing.T) {
	c := client{options: clientOptions{validator: nil}}

	// A non-SelfValidator on the default client has nothing to validate and
	// must not panic on the nil StructValidator fallback.
	if err := c.validateStruct(context.Background(), &plainEntity{Name: "x"}); err != nil {
		t.Fatalf("plain entity on the default client should be a no-op, got %v", err)
	}
}
