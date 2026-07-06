/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package dgdao_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Three-level hierarchy for testing managed reverse edges:
// Department -> Course -> Enrollment
//
// Traditional forward edges (what gets stored in Dgraph):
//   Enrollment.in_course -> Course
//   Course.in_department -> Department
//
// Managed reverse edges (defined on parent, creates forward edges on children):
//   Department.Courses -> uses ~in_department
//   Course.Enrollments -> uses ~in_course

// Level 3 (bottom): Enrollment has forward edge to Course
type Enrollment struct {
	UID       string    `json:"uid,omitempty"`
	StudentID string    `json:"student_id,omitempty" dgraph:"index=hash"`
	Grade     string    `json:"grade,omitempty"`
	InCourse  []*Course `json:"in_course,omitempty" dgraph:"reverse"`
	DType     []string  `json:"dgraph.type,omitempty"`
}

// Level 2 (middle): Course has forward edge to Department (single), managed reverse to Enrollments
type Course struct {
	UID  string `json:"uid,omitempty"`
	Name string `json:"course_name,omitempty" dgraph:"index=term,hash"`
	Code string `json:"code,omitempty" dgraph:"index=exact"`
	/* trunk-ignore(golangci-lint/lll) */
	InDepartment *Department   `json:"in_department,omitempty" dgraph:"reverse"` // single edge (course belongs to one department)
	Enrollments  []*Enrollment `json:"~in_course,omitempty" dgraph:"reverse"`    // managed reverse edge
	DType        []string      `json:"dgraph.type,omitempty"`
}

// Level 1 (top): Department has managed reverse edge to Courses
type Department struct {
	UID     string    `json:"uid,omitempty"`
	Name    string    `json:"dept_name,omitempty" dgraph:"index=term,hash unique upsert"` // unique for upsert support
	Budget  int       `json:"budget,omitempty"`
	Courses []*Course `json:"~in_department,omitempty" dgraph:"reverse"` // managed reverse edge
	DType   []string  `json:"dgraph.type,omitempty"`
}

func TestReverseEdgeMutateFromTop(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "MutateFromTopWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "MutateFromTopWithDgraphURI",
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

			ctx := context.Background()
			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			// Create a department with courses and enrollments from the top level
			dept := &Department{
				Name:   "Computer Science",
				Budget: 1000000,
				Courses: []*Course{
					{
						Name: "Algorithms",
						Code: "CS101",
						Enrollments: []*Enrollment{
							{StudentID: "S001", Grade: "A"},
							{StudentID: "S002", Grade: "B"},
						},
					},
					{
						Name: "Data Structures",
						Code: "CS102",
						Enrollments: []*Enrollment{
							{StudentID: "S001", Grade: "A+"},
							{StudentID: "S003", Grade: "C"},
						},
					},
				},
			}

			err := client.Insert(ctx, dept)
			require.NoError(t, err)

			// Verify UIDs were assigned
			assert.NotEmpty(t, dept.UID, "Department should have UID")
			assert.NotEmpty(t, dept.Courses[0].UID, "Course should have UID")
			assert.NotEmpty(t, dept.Courses[0].Enrollments[0].UID, "Enrollment should have UID")
		})
	}
}

func TestReverseEdgeQueryFromTop(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "QueryFromTopWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "QueryFromTopWithDgraphURI",
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

			ctx := context.Background()
			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			// Create enrollments first with UIDs
			enrollment1 := &Enrollment{StudentID: "S001", Grade: "A"}
			enrollment2 := &Enrollment{StudentID: "S002", Grade: "B"}

			err := client.Insert(ctx, enrollment1)
			require.NoError(t, err)

			err = client.Insert(ctx, enrollment2)
			require.NoError(t, err)

			// Create course with enrollments (this should create in_course edges on enrollments)
			course := &Course{
				Name:        "Algorithms",
				Code:        "CS101",
				Enrollments: []*Enrollment{enrollment1, enrollment2},
			}

			err = client.Insert(ctx, course)
			require.NoError(t, err)

			// Verify forward edge was created: query enrollment and check in_course
			var gotEnrollment Enrollment
			err = client.Get(ctx, &gotEnrollment, enrollment1.UID)
			require.NoError(t, err)
			require.Len(t, gotEnrollment.InCourse, 1, "Enrollment should have in_course edge to Course")
			assert.Equal(t, course.UID, gotEnrollment.InCourse[0].UID, "in_course should point to the course")

			// Create department with course
			dept := &Department{
				Name:    "Computer Science",
				Budget:  1000000,
				Courses: []*Course{course},
			}

			err = client.Insert(ctx, dept)
			require.NoError(t, err)

			// Query the department and verify reverse edges are populated
			var result Department
			err = client.Get(ctx, &result, dept.UID)
			require.NoError(t, err)

			assert.Equal(t, "Computer Science", result.Name)
			assert.Len(t, result.Courses, 1, "Should have 1 course via reverse edge")
			assert.Equal(t, "Algorithms", result.Courses[0].Name)
			assert.Len(t, result.Courses[0].Enrollments, 2, "Should have 2 enrollments via reverse edge")
		})
	}
}

func TestReverseEdgeQueryFromMiddle(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "QueryFromMiddleWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "QueryFromMiddleWithDgraphURI",
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

			ctx := context.Background()
			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			// Create test data
			dept := &Department{
				Name:   "Mathematics",
				Budget: 500000,
				Courses: []*Course{
					{
						Name: "Calculus",
						Code: "MATH101",
						Enrollments: []*Enrollment{
							{StudentID: "S001", Grade: "A"},
							{StudentID: "S002", Grade: "B+"},
							{StudentID: "S003", Grade: "A-"},
						},
					},
				},
			}

			err := client.Insert(ctx, dept)
			require.NoError(t, err)

			// Query a course directly
			var result Course
			err = client.Get(ctx, &result, dept.Courses[0].UID)
			require.NoError(t, err)

			assert.Equal(t, "Calculus", result.Name)
			assert.Equal(t, "MATH101", result.Code)
			assert.Len(t, result.Enrollments, 3, "Should have 3 enrollments via reverse edge")
		})
	}
}

func TestReverseEdgeNavigateFromBottom(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "NavigateFromBottomWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "NavigateFromBottomWithDgraphURI",
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

			ctx := context.Background()
			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			// Create full hierarchy
			dept := &Department{
				Name:   "Physics",
				Budget: 900000,
				Courses: []*Course{
					{
						Name: "Mechanics",
						Code: "PHYS101",
						Enrollments: []*Enrollment{
							{StudentID: "P001", Grade: "A"},
							{StudentID: "P002", Grade: "B+"},
						},
					},
				},
			}

			err := client.Insert(ctx, dept)
			require.NoError(t, err)

			enrollmentUID := dept.Courses[0].Enrollments[0].UID
			courseUID := dept.Courses[0].UID
			deptUID := dept.UID

			// Query starting from Enrollment, navigate up to Course, then to Department
			var enrollment Enrollment
			err = client.Get(ctx, &enrollment, enrollmentUID)
			require.NoError(t, err)

			// Verify we can navigate Enrollment -> Course
			assert.Equal(t, "P001", enrollment.StudentID)
			require.Len(t, enrollment.InCourse, 1, "Enrollment should have in_course edge")
			assert.Equal(t, courseUID, enrollment.InCourse[0].UID)
			assert.Equal(t, "Mechanics", enrollment.InCourse[0].Name)

			// Verify we can navigate Course -> Department
			require.NotNil(t, enrollment.InCourse[0].InDepartment, "Course should have in_department edge")
			assert.Equal(t, deptUID, enrollment.InCourse[0].InDepartment.UID)
			assert.Equal(t, "Physics", enrollment.InCourse[0].InDepartment.Name)
		})
	}
}

func TestReverseEdgeAddCourseToExistingDepartment(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "AddCourseWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "AddCourseWithDgraphURI",
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

			ctx := context.Background()
			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			// Create a department with one course
			dept := &Department{
				Name:   "Engineering",
				Budget: 1000000,
				Courses: []*Course{
					{
						Name: "Statics",
						Code: "ENG101",
						Enrollments: []*Enrollment{
							{StudentID: "E001", Grade: "A"},
						},
					},
				},
			}

			err := client.Insert(ctx, dept)
			require.NoError(t, err)

			// Verify initial state: 1 course
			var result Department
			err = client.Get(ctx, &result, dept.UID)
			require.NoError(t, err)
			require.Len(t, result.Courses, 1, "Should have 1 course initially")
			assert.Equal(t, "Statics", result.Courses[0].Name)

			// Now add a second course to the existing department
			newCourse := &Course{
				Name: "Dynamics",
				Code: "ENG102",
				Enrollments: []*Enrollment{
					{StudentID: "E002", Grade: "B"},
					{StudentID: "E003", Grade: "A-"},
				},
			}

			// Update department with new course added
			dept.Courses = append(dept.Courses, newCourse)

			err = client.Upsert(ctx, dept, "dept_name")
			require.NoError(t, err)

			// Verify: department now has 2 courses
			var updated Department
			err = client.Get(ctx, &updated, dept.UID)
			require.NoError(t, err)

			assert.Equal(t, "Engineering", updated.Name)
			require.Len(t, updated.Courses, 2, "Should have 2 courses after update")

			// Verify both courses exist with correct enrollments
			courseNames := make(map[string]int)
			for _, course := range updated.Courses {
				courseNames[course.Name] = len(course.Enrollments)
			}

			assert.Equal(t, 1, courseNames["Statics"], "Statics should have 1 enrollment")
			assert.Equal(t, 2, courseNames["Dynamics"], "Dynamics should have 2 enrollments")

			// Also verify new course has UID assigned
			assert.NotEmpty(t, newCourse.UID, "New course should have UID assigned")
		})
	}
}

func TestReverseEdgeUpsert(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "UpsertWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "UpsertWithDgraphURI",
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

			ctx := context.Background()
			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			// Create initial department
			dept := &Department{
				Name:   "Chemistry",
				Budget: 500000,
				Courses: []*Course{
					{Name: "Organic Chemistry", Code: "CHEM201"},
				},
			}

			err := client.Insert(ctx, dept)
			require.NoError(t, err)

			originalUID := dept.UID
			assert.NotEmpty(t, originalUID)

			// Upsert same department name with different budget and additional course
			upsertDept := &Department{
				Name:   "Chemistry", // same name - should match existing
				Budget: 750000,      // updated budget
				Courses: []*Course{
					{Name: "Inorganic Chemistry", Code: "CHEM202"}, // new course
				},
			}

			err = client.Upsert(ctx, upsertDept, "dept_name")
			require.NoError(t, err)

			// Should reuse the same UID
			assert.Equal(t, originalUID, upsertDept.UID, "Upsert should reuse existing UID")

			// Verify the department was updated
			var result Department
			err = client.Get(ctx, &result, originalUID)
			require.NoError(t, err)

			assert.Equal(t, "Chemistry", result.Name)
			assert.Equal(t, 750000, result.Budget, "Budget should be updated")

			// Should have both courses (original + new from upsert)
			require.Len(t, result.Courses, 2, "Should have 2 courses after upsert")

			courseNames := make(map[string]bool)
			for _, course := range result.Courses {
				courseNames[course.Name] = true
			}
			assert.True(t, courseNames["Organic Chemistry"], "Should have original course")
			assert.True(t, courseNames["Inorganic Chemistry"], "Should have new course from upsert")
		})
	}
}

func TestReverseEdgeUpsertPreservesEdges(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "UpsertPreservesEdgesWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "UpsertPreservesEdgesWithDgraphURI",
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

			ctx := context.Background()
			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			// First: create department with courses
			dept := &Department{
				Name:   "Biology",
				Budget: 600000,
				Courses: []*Course{
					{
						Name: "Genetics",
						Code: "BIO301",
						Enrollments: []*Enrollment{
							{StudentID: "B001", Grade: "A"},
							{StudentID: "B002", Grade: "B+"},
						},
					},
					{
						Name: "Microbiology",
						Code: "BIO302",
						Enrollments: []*Enrollment{
							{StudentID: "B003", Grade: "A-"},
						},
					},
				},
			}

			err := client.Insert(ctx, dept)
			require.NoError(t, err)

			deptUID := dept.UID

			// Upsert with empty Courses - should NOT wipe existing courses
			upsertDept := &Department{
				Name:   "Biology", // same name
				Budget: 700000,    // budget increase
				// Courses intentionally empty/nil
			}

			err = client.Upsert(ctx, upsertDept, "dept_name")
			require.NoError(t, err)

			assert.Equal(t, deptUID, upsertDept.UID, "Should still match existing department")

			// Verify courses are preserved
			var result Department
			err = client.Get(ctx, &result, deptUID)
			require.NoError(t, err)

			assert.Equal(t, "Biology", result.Name)
			assert.Equal(t, 700000, result.Budget, "Budget should be updated")
			require.Len(t, result.Courses, 2, "Should still have 2 courses - upsert with empty Courses should not wipe edges")
		})
	}
}

func TestInsertMultipleEntitiesWithReverseEdges(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "InsertMultipleReverseWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "InsertMultipleReverseWithDgraphURI",
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

			ctx := context.Background()
			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			// Insert a slice of Department pointers, each with embedded reverse edge structs
			departments := []*Department{
				{
					Name:   "Computer Science",
					Budget: 1000000,
					Courses: []*Course{
						{
							Name: "Algorithms",
							Code: "CS101",
							Enrollments: []*Enrollment{
								{StudentID: "S001", Grade: "A"},
								{StudentID: "S002", Grade: "B"},
							},
						},
						{
							Name: "Data Structures",
							Code: "CS102",
							Enrollments: []*Enrollment{
								{StudentID: "S003", Grade: "A+"},
							},
						},
					},
				},
				{
					Name:   "Mathematics",
					Budget: 800000,
					Courses: []*Course{
						{
							Name: "Calculus",
							Code: "MATH101",
							Enrollments: []*Enrollment{
								{StudentID: "M001", Grade: "B+"},
								{StudentID: "M002", Grade: "A-"},
							},
						},
					},
				},
				{
					Name:   "Physics",
					Budget: 900000,
					Courses: []*Course{
						{
							Name: "Mechanics",
							Code: "PHYS101",
							Enrollments: []*Enrollment{
								{StudentID: "P001", Grade: "A"},
							},
						},
						{
							Name: "Electromagnetism",
							Code: "PHYS201",
							Enrollments: []*Enrollment{
								{StudentID: "P002", Grade: "B"},
								{StudentID: "P003", Grade: "A"},
							},
						},
					},
				},
			}

			err := client.Insert(ctx, departments)
			require.NoError(t, err, "Insert slice of departments should succeed")

			// Verify all departments got UIDs assigned
			for i, dept := range departments {
				require.NotEmpty(t, dept.UID, "Department %d should have UID", i)

				// Verify all courses got UIDs
				for j, course := range dept.Courses {
					require.NotEmpty(t, course.UID, "Course %d in Department %d should have UID", j, i)

					// Verify all enrollments got UIDs
					for k, enrollment := range course.Enrollments {
						require.NotEmpty(t, enrollment.UID, "Enrollment %d in Course %d should have UID", k, j)
					}
				}
			}

			// Verify we can retrieve each department with its nested structs via reverse edges
			for _, original := range departments {
				var retrieved Department
				err = client.Get(ctx, &retrieved, original.UID)
				require.NoError(t, err, "Get should succeed for %s", original.Name)

				assert.Equal(t, original.Name, retrieved.Name, "Department name should match")
				assert.Equal(t, original.Budget, retrieved.Budget, "Budget should match")
				require.Len(t, retrieved.Courses, len(original.Courses), "Course count should match")

				// Verify reverse edges are properly populated
				for _, course := range retrieved.Courses {
					require.NotEmpty(t, course.UID, "Retrieved course should have UID")
					require.NotEmpty(t, course.Name, "Retrieved course should have name")

					// Verify enrollments via reverse edge
					for _, enrollment := range course.Enrollments {
						require.NotEmpty(t, enrollment.UID, "Retrieved enrollment should have UID")
						require.NotEmpty(t, enrollment.StudentID, "Retrieved enrollment should have StudentID")
					}
				}
			}

			// Query all departments and verify count
			var results []Department
			err = client.Query(ctx, Department{}).Nodes(&results)
			require.NoError(t, err, "Query should succeed")
			assert.Len(t, results, 3, "Should have found all three departments")
		})
	}
}

// FoafPerson struct for Friend of a Friend graph testing
// Self-referential with bidirectional friends relationship using @reverse
type FoafPerson struct {
	UID       string        `json:"uid,omitempty"`
	Name      string        `json:"person_name,omitempty" dgraph:"index=term,hash"`
	Friends   []*FoafPerson `json:"friends,omitempty" dgraph:"reverse"`  // Forward edge with @reverse in schema
	FriendsOf []*FoafPerson `json:"~friends,omitempty" dgraph:"reverse"` // Reverse edge for queries
	DType     []string      `json:"dgraph.type,omitempty"`
}

func TestFriendOfFriendBidirectional(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "BidirectionalWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "BidirectionalWithDgraphURI",
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

			ctx := context.Background()
			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			// Create Sally first
			sally := &FoafPerson{Name: "Sally"}
			err := client.Insert(ctx, sally)
			require.NoError(t, err)
			require.NotEmpty(t, sally.UID, "Sally should have UID")

			// Create Bob with Sally as a friend (tests self-referential mutation)
			bob := &FoafPerson{
				Name:    "Bob",
				Friends: []*FoafPerson{sally},
			}
			err = client.Insert(ctx, bob)
			require.NoError(t, err)
			require.NotEmpty(t, bob.UID, "Bob should have UID")

			// Query Bob - should have Sally as friend
			var gotBob FoafPerson
			err = client.Get(ctx, &gotBob, bob.UID)
			require.NoError(t, err)
			assert.Equal(t, "Bob", gotBob.Name)
			require.Len(t, gotBob.Friends, 1, "Bob should have 1 friend")
			assert.Equal(t, "Sally", gotBob.Friends[0].Name)

			// Query Sally - should have Bob in FriendsOf (reverse edge)
			var gotSally FoafPerson
			err = client.Get(ctx, &gotSally, sally.UID)
			require.NoError(t, err)
			assert.Equal(t, "Sally", gotSally.Name)
			require.Len(t, gotSally.FriendsOf, 1, "Sally should have 1 person who friended her")
			assert.Equal(t, "Bob", gotSally.FriendsOf[0].Name)
		})
	}
}

func TestFriendOfFriendChain(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "ChainWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "ChainWithDgraphURI",
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

			ctx := context.Background()
			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			// Create chain: Alice -> Bob -> Carol -> Dave
			dave := &FoafPerson{Name: "Dave"}
			err := client.Insert(ctx, dave)
			require.NoError(t, err)

			carol := &FoafPerson{Name: "Carol", Friends: []*FoafPerson{dave}}
			err = client.Insert(ctx, carol)
			require.NoError(t, err)
			require.NotEmpty(t, carol.UID, "Carol should have UID")

			bob := &FoafPerson{Name: "Bob", Friends: []*FoafPerson{carol}}
			err = client.Insert(ctx, bob)
			require.NoError(t, err)
			require.NotEmpty(t, bob.UID, "Bob should have UID")

			alice := &FoafPerson{Name: "Alice", Friends: []*FoafPerson{bob}}
			err = client.Insert(ctx, alice)
			require.NoError(t, err)
			require.NotEmpty(t, alice.UID, "Alice should have UID")

			// Query Alice - verify forward chain: Alice -> Bob -> Carol -> Dave
			var gotAlice FoafPerson
			err = client.Get(ctx, &gotAlice, alice.UID)
			require.NoError(t, err)

			assert.Equal(t, "Alice", gotAlice.Name)
			require.Len(t, gotAlice.Friends, 1)
			assert.Equal(t, "Bob", gotAlice.Friends[0].Name)
			require.Len(t, gotAlice.Friends[0].Friends, 1)
			assert.Equal(t, "Carol", gotAlice.Friends[0].Friends[0].Name)
			require.Len(t, gotAlice.Friends[0].Friends[0].Friends, 1)
			assert.Equal(t, "Dave", gotAlice.Friends[0].Friends[0].Friends[0].Name)

			// Query Dave and verify reverse edge (one level)
			var gotDave FoafPerson
			err = client.Get(ctx, &gotDave, dave.UID)
			require.NoError(t, err)

			assert.Equal(t, "Dave", gotDave.Name)
			require.Len(t, gotDave.FriendsOf, 1, "Dave should have 1 person who friended him")
			assert.Equal(t, "Carol", gotDave.FriendsOf[0].Name)
		})
	}
}

func TestFriendOfFriendMutualFriends(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "MutualFriendsWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "MutualFriendsWithDgraphURI",
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

			ctx := context.Background()
			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			// Create people
			alice := &FoafPerson{Name: "Alice"}
			bob := &FoafPerson{Name: "Bob"}
			carol := &FoafPerson{Name: "Carol"}

			for _, p := range []*FoafPerson{alice, bob, carol} {
				err := client.Insert(ctx, p)
				require.NoError(t, err)
			}

			// Alice and Bob both friend Carol
			alice.Friends = []*FoafPerson{carol}
			err := client.Insert(ctx, alice)
			require.NoError(t, err)

			bob.Friends = []*FoafPerson{carol}
			err = client.Insert(ctx, bob)
			require.NoError(t, err)

			// Query Carol - should have both Alice and Bob in FriendsOf
			var gotCarol FoafPerson
			err = client.Get(ctx, &gotCarol, carol.UID)
			require.NoError(t, err)

			assert.Equal(t, "Carol", gotCarol.Name)
			require.Len(t, gotCarol.FriendsOf, 2, "Carol should have 2 people who friended her")

			friendNames := make(map[string]bool)
			for _, f := range gotCarol.FriendsOf {
				friendNames[f.Name] = true
			}
			assert.True(t, friendNames["Alice"], "Alice should have friended Carol")
			assert.True(t, friendNames["Bob"], "Bob should have friended Carol")
		})
	}
}

func TestFriendOfFriendChainEmbedded(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "ChainEmbeddedWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "ChainEmbeddedWithDgraphURI",
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

			ctx := context.Background()
			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			// Create chain Alice -> Bob -> Carol -> Dave with directly embedded objects
			alice := &FoafPerson{
				Name: "Alice",
				Friends: []*FoafPerson{
					{
						Name: "Bob",
						Friends: []*FoafPerson{
							{
								Name: "Carol",
								Friends: []*FoafPerson{
									{Name: "Dave"},
								},
							},
						},
					},
				},
			}

			err := client.Insert(ctx, alice)
			require.NoError(t, err)

			// Verify all UIDs assigned
			require.NotEmpty(t, alice.UID, "Alice should have UID")
			bob := alice.Friends[0]
			require.NotEmpty(t, bob.UID, "Bob should have UID")
			carol := bob.Friends[0]
			require.NotEmpty(t, carol.UID, "Carol should have UID")
			dave := carol.Friends[0]
			require.NotEmpty(t, dave.UID, "Dave should have UID")

			// Query Alice - verify forward chain: Alice -> Bob -> Carol -> Dave
			var gotAlice FoafPerson
			err = client.Get(ctx, &gotAlice, alice.UID)
			require.NoError(t, err)

			assert.Equal(t, "Alice", gotAlice.Name)
			require.Len(t, gotAlice.Friends, 1)
			assert.Equal(t, "Bob", gotAlice.Friends[0].Name)
			require.Len(t, gotAlice.Friends[0].Friends, 1)
			assert.Equal(t, "Carol", gotAlice.Friends[0].Friends[0].Name)
			require.Len(t, gotAlice.Friends[0].Friends[0].Friends, 1)
			assert.Equal(t, "Dave", gotAlice.Friends[0].Friends[0].Friends[0].Name)

			// Query Dave - verify reverse edge chain: Dave <- Carol <- Bob <- Alice
			var gotDave FoafPerson
			err = client.Get(ctx, &gotDave, dave.UID)
			require.NoError(t, err)

			assert.Equal(t, "Dave", gotDave.Name)
			require.Len(t, gotDave.FriendsOf, 1, "Dave should have 1 person who friended him")
			assert.Equal(t, "Carol", gotDave.FriendsOf[0].Name)
			require.Len(t, gotDave.FriendsOf[0].FriendsOf, 1, "Carol should have 1 person who friended her")
			assert.Equal(t, "Bob", gotDave.FriendsOf[0].FriendsOf[0].Name)
			require.Len(t, gotDave.FriendsOf[0].FriendsOf[0].FriendsOf, 1, "Bob should have 1 person who friended him")
			assert.Equal(t, "Alice", gotDave.FriendsOf[0].FriendsOf[0].FriendsOf[0].Name)
		})
	}
}

func TestFriendOfFriendQueryByName(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "QueryByNameWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "QueryByNameWithDgraphURI",
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

			ctx := context.Background()
			client, cleanup := CreateTestClient(t, tc.uri)
			defer cleanup()

			// Build chain: Alice -> Bob -> Carol -> Dave
			dave := &FoafPerson{Name: "Dave"}
			err := client.Insert(ctx, dave)
			require.NoError(t, err)

			carol := &FoafPerson{Name: "Carol", Friends: []*FoafPerson{dave}}
			err = client.Insert(ctx, carol)
			require.NoError(t, err)

			bob := &FoafPerson{Name: "Bob", Friends: []*FoafPerson{carol}}
			err = client.Insert(ctx, bob)
			require.NoError(t, err)

			alice := &FoafPerson{Name: "Alice", Friends: []*FoafPerson{bob}}
			err = client.Insert(ctx, alice)
			require.NoError(t, err)

			// Query Carol by name
			var gotCarol FoafPerson
			err = client.Query(ctx, FoafPerson{}).Filter(`eq(person_name, "Carol")`).Node(&gotCarol)
			require.NoError(t, err)

			assert.Equal(t, "Carol", gotCarol.Name)

			// Verify forward path: Carol -> Dave
			require.Len(t, gotCarol.Friends, 1, "Carol should have 1 friend (Dave)")
			assert.Equal(t, "Dave", gotCarol.Friends[0].Name)

			// Verify reverse path: Bob -> Carol (Bob friended Carol)
			require.Len(t, gotCarol.FriendsOf, 1, "Carol should have 1 person who friended her (Bob)")
			assert.Equal(t, "Bob", gotCarol.FriendsOf[0].Name)

			// Verify nested reverse path: Alice -> Bob (Alice friended Bob)
			require.Len(t, gotCarol.FriendsOf[0].FriendsOf, 1, "Bob should have 1 person who friended him (Alice)")
			assert.Equal(t, "Alice", gotCarol.FriendsOf[0].FriendsOf[0].Name)

			// Query Bob by name and ensure forward/reverse edges are correct
			var gotBob FoafPerson
			err = client.Query(ctx, FoafPerson{}).Filter(`eq(person_name, "Bob")`).Node(&gotBob)
			require.NoError(t, err)
			require.Len(t, gotBob.Friends, 1, "Bob should have 1 friend (Carol)")
			require.Len(t, gotBob.FriendsOf, 1, "Bob should have 1 person who friended him (Alice)")
			require.Equal(t, "Carol", gotBob.Friends[0].Name)
			require.Equal(t, "Alice", gotBob.FriendsOf[0].Name)
			// Alice is at the start of the chain - nobody friended her
			require.Len(t, gotBob.FriendsOf[0].FriendsOf, 0, "Alice should have no one who friended her")
			// Verify forward chain: Bob -> Carol -> Dave
			require.Len(t, gotBob.Friends[0].Friends, 1, "Carol should have 1 friend (Dave)")
			require.Equal(t, "Dave", gotBob.Friends[0].Friends[0].Name)
		})
	}
}
