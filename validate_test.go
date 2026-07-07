/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"os"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mg "github.com/dgraph-io/dgdao"
)

// ValidatableUser is a test struct with validation tags
type ValidatableUser struct {
	UID    string   `json:"uid,omitempty"`
	Name   string   `json:"name,omitempty" validate:"required,min=2,max=100"`
	Email  string   `json:"email,omitempty" validate:"required,email"`
	Age    int      `json:"age,omitempty" validate:"gte=0,lte=130"`
	Status string   `json:"status,omitempty" validate:"oneof=active inactive pending"`
	DType  []string `json:"dgraph.type,omitempty"`
}

// CustomValidatableEntity tests custom validation rules
type CustomValidatableEntity struct {
	UID     string   `json:"uid,omitempty"`
	Code    string   `json:"code,omitempty" validate:"required,custom_code"`
	Enabled bool     `json:"enabled,omitempty"`
	DType   []string `json:"dgraph.type,omitempty"`
}

func TestClientWithValidator(t *testing.T) {

	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "ValidatorWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "ValidatorWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("DGDAO_TEST_ADDR"),
			skip: os.Getenv("DGDAO_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("Skipping %s: DGDAO_TEST_ADDR not set", tc.name)
				return
			}

			// Create a validator instance
			validate := validator.New()

			// Register custom validation
			err := validate.RegisterValidation("custom_code", func(fl validator.FieldLevel) bool {
				code := fl.Field().String()
				return len(code) == 6 && code[0:3] == "ABC"
			})
			require.NoError(t, err)

			// Create client with validator
			client, err := mg.NewClient(tc.uri, mg.WithAutoSchema(true), mg.WithValidator(validate))
			require.NoError(t, err)
			defer client.Close()

			ctx := context.Background()

			t.Run("ValidEntityShouldPass", func(t *testing.T) {
				user := ValidatableUser{
					Name:   "John Doe",
					Email:  "john.doe@example.com",
					Age:    30,
					Status: "active",
				}

				err := client.Insert(ctx, &user)
				require.NoError(t, err, "Valid entity should insert successfully")
				require.NotEmpty(t, user.UID, "UID should be assigned")
			})

			t.Run("InvalidEntityShouldFail", func(t *testing.T) {
				user := ValidatableUser{
					Name:   "",              // Invalid: empty name (required)
					Email:  "invalid-email", // Invalid: not a valid email
					Age:    150,             // Invalid: age > 130
					Status: "unknown",       // Invalid: not oneof allowed values
				}

				err := client.Insert(ctx, &user)
				require.Error(t, err, "Invalid entity should fail validation")
				assert.Contains(t, err.Error(), "validation", "Error should mention validation")
				assert.Empty(t, user.UID, "UID should not be assigned for failed validation")
			})

			t.Run("PartialInvalidEntityShouldFail", func(t *testing.T) {
				user := ValidatableUser{
					Name:   "J", // Invalid: name too short (min=2)
					Email:  "valid@example.com",
					Age:    25,
					Status: "active",
				}

				err := client.Insert(ctx, &user)
				require.Error(t, err, "Partially invalid entity should fail validation")
				assert.Empty(t, user.UID, "UID should not be assigned for failed validation")
			})

			t.Run("CustomValidationShouldWork", func(t *testing.T) {
				entity := CustomValidatableEntity{
					Code:    "ABC123", // Valid: starts with ABC and 6 chars
					Enabled: true,
				}

				err := client.Insert(ctx, &entity)
				require.NoError(t, err, "Entity with valid custom code should insert successfully")
				require.NotEmpty(t, entity.UID, "UID should be assigned")
			})

			t.Run("CustomValidationShouldFail", func(t *testing.T) {
				entity := CustomValidatableEntity{
					Code:    "XYZ123", // Invalid: doesn't start with ABC
					Enabled: true,
				}

				err := client.Insert(ctx, &entity)
				require.Error(t, err, "Entity with invalid custom code should fail validation")
				assert.Empty(t, entity.UID, "UID should not be assigned for failed validation")
			})

			t.Run("UpdateWithValidation", func(t *testing.T) {
				user := ValidatableUser{
					Name:   "Jane Smith",
					Email:  "jane.smith@example.com",
					Age:    28,
					Status: "active",
				}

				// Insert valid user first
				err := client.Insert(ctx, &user)
				require.NoError(t, err)

				// Try to update with invalid data
				user.Email = "invalid-email-updated"
				err = client.Update(ctx, &user)
				require.Error(t, err, "Update with invalid data should fail validation")

				// Verify original data is still intact
				retrieved := ValidatableUser{UID: user.UID}
				err = client.Get(ctx, &retrieved, user.UID)
				require.NoError(t, err)
				assert.Equal(t, "jane.smith@example.com", retrieved.Email, "Original email should be preserved")
			})

			t.Run("UpsertWithValidation", func(t *testing.T) {
				user := ValidatableUser{
					Name:   "Bob Wilson",
					Email:  "bob.wilson@example.com",
					Age:    35,
					Status: "pending",
				}

				// Upsert valid user
				err := client.Upsert(ctx, &user)
				require.NoError(t, err, "Valid upsert should succeed")
				require.NotEmpty(t, user.UID, "UID should be assigned")

				// Try to upsert with invalid data
				invalidUser := ValidatableUser{
					Name:   "",                       // Invalid: empty name
					Email:  "bob.wilson@example.com", // Same email to trigger upsert path
					Age:    35,
					Status: "pending",
				}

				err = client.Upsert(ctx, &invalidUser)
				require.Error(t, err, "Upsert with invalid data should fail validation")
			})
		})
	}
}

func TestValidatorWithoutAutoSchema(t *testing.T) {
	// Test validator behavior when AutoSchema is disabled
	validate := validator.New()
	client, err := mg.NewClient("file://"+GetTempDir(t), mg.WithValidator(validate))
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	user := ValidatableUser{
		Name:   "Test User",
		Email:  "test@example.com",
		Age:    25,
		Status: "active",
	}

	// Should still validate even without AutoSchema
	err = client.Insert(ctx, &user)
	require.Error(t, err, "Insert should fail without schema but validation should still run")
	assert.Contains(t, err.Error(), "schema", "Error should mention schema issue")
}
