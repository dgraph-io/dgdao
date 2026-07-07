/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package load_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/dgraph-io/dgdao"
	"github.com/go-logr/stdr"
	"github.com/stretchr/testify/require"
)

// BenchmarkEntity represents a test entity for the long-running benchmark
type BenchmarkEntity struct {
	UID         string    `json:"uid,omitempty"`
	Name        string    `json:"name,omitempty" dgraph:"index=term,exact"`
	Description string    `json:"description,omitempty" dgraph:"index=term"`
	Value       int       `json:"value,omitempty" dgraph:"index=int"`
	Score       float64   `json:"score,omitempty" dgraph:"index=float"`
	CreatedAt   time.Time `json:"createdAt,omitempty" dgraph:"index=day"`
	UpdatedAt   time.Time `json:"updatedAt,omitempty"`
	Tags        []string  `json:"tags,omitempty" dgraph:"index=term"`
	DType       []string  `json:"dgraph.type,omitempty"`
}

// OperationStats tracks metrics for different operations
type OperationStats struct {
	mu              sync.RWMutex
	writeCount      int64
	writeErrors     int64
	writeTotalTime  time.Duration
	updateCount     int64
	updateErrors    int64
	updateTotalTime time.Duration
	deleteCount     int64
	deleteErrors    int64
	deleteTotalTime time.Duration
	queryCount      int64
	queryErrors     int64
	queryTotalTime  time.Duration
	entitiesInDB    atomic.Int64
	lastReportTime  time.Time
	startTime       time.Time
}

// RecordWrite records write operation metrics
func (s *OperationStats) RecordWrite(duration time.Duration, success bool, count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeCount++
	s.writeTotalTime += duration
	if !success {
		s.writeErrors++
	} else {
		s.entitiesInDB.Add(int64(count))
	}
}

// RecordUpdate records update operation metrics
func (s *OperationStats) RecordUpdate(duration time.Duration, success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateCount++
	s.updateTotalTime += duration
	if !success {
		s.updateErrors++
	}
}

// RecordDelete records delete operation metrics
func (s *OperationStats) RecordDelete(duration time.Duration, success bool, count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteCount++
	s.deleteTotalTime += duration
	if !success {
		s.deleteErrors++
	} else {
		s.entitiesInDB.Add(-int64(count))
	}
}

// RecordQuery records query operation metrics
func (s *OperationStats) RecordQuery(duration time.Duration, success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queryCount++
	s.queryTotalTime += duration
	if !success {
		s.queryErrors++
	}
}

// Report generates a performance report
func (s *OperationStats) Report() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	elapsed := now.Sub(s.lastReportTime)
	totalElapsed := now.Sub(s.startTime)

	// Get memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	allocMB := float64(m.Alloc) / 1024 / 1024
	totalAllocMB := float64(m.TotalAlloc) / 1024 / 1024
	sysMB := float64(m.Sys) / 1024 / 1024

	report := fmt.Sprintf("\n=== Performance Report (%.1fs interval, %.1fs total) ===\n",
		elapsed.Seconds(), totalElapsed.Seconds())
	report += fmt.Sprintf("Entities in DB: %d | Memory: Alloc=%.1fMB, TotalAlloc=%.1fMB, Sys=%.1fMB\n\n",
		s.entitiesInDB.Load(), allocMB, totalAllocMB, sysMB)

	if s.writeCount > 0 {
		avgWrite := s.writeTotalTime / time.Duration(s.writeCount)
		report += fmt.Sprintf("Writes:  %6d ops | Avg: %8s | Errors: %3d | Rate: %.2f ops/s\n",
			s.writeCount, avgWrite, s.writeErrors, float64(s.writeCount)/totalElapsed.Seconds())
	}

	if s.updateCount > 0 {
		avgUpdate := s.updateTotalTime / time.Duration(s.updateCount)
		report += fmt.Sprintf("Updates: %6d ops | Avg: %8s | Errors: %3d | Rate: %.2f ops/s\n",
			s.updateCount, avgUpdate, s.updateErrors, float64(s.updateCount)/totalElapsed.Seconds())
	}

	if s.deleteCount > 0 {
		avgDelete := s.deleteTotalTime / time.Duration(s.deleteCount)
		report += fmt.Sprintf("Deletes: %6d ops | Avg: %8s | Errors: %3d | Rate: %.2f ops/s\n",
			s.deleteCount, avgDelete, s.deleteErrors, float64(s.deleteCount)/totalElapsed.Seconds())
	}

	if s.queryCount > 0 {
		avgQuery := s.queryTotalTime / time.Duration(s.queryCount)
		report += fmt.Sprintf("Queries: %6d ops | Avg: %8s | Errors: %3d | Rate: %.2f ops/s\n",
			s.queryCount, avgQuery, s.queryErrors, float64(s.queryCount)/totalElapsed.Seconds())
	}

	report += "========================================\n"
	return report
}

// ResetTimer resets the last report time
func (s *OperationStats) ResetTimer() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastReportTime = time.Now()
}

// GetFinalStats returns a map of final statistics
func (s *OperationStats) GetFinalStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	totalElapsed := time.Since(s.startTime).Seconds()

	return map[string]interface{}{
		"duration_seconds":      totalElapsed,
		"entities_in_db":        s.entitiesInDB.Load(),
		"write_count":           s.writeCount,
		"write_errors":          s.writeErrors,
		"write_avg_ms":          float64(s.writeTotalTime.Milliseconds()) / max(float64(s.writeCount), 1),
		"write_ops_per_sec":     float64(s.writeCount) / totalElapsed,
		"update_count":          s.updateCount,
		"update_errors":         s.updateErrors,
		"update_avg_ms":         float64(s.updateTotalTime.Milliseconds()) / max(float64(s.updateCount), 1),
		"update_ops_per_sec":    float64(s.updateCount) / totalElapsed,
		"delete_count":          s.deleteCount,
		"delete_errors":         s.deleteErrors,
		"delete_avg_ms":         float64(s.deleteTotalTime.Milliseconds()) / max(float64(s.deleteCount), 1),
		"delete_ops_per_sec":    float64(s.deleteCount) / totalElapsed,
		"query_count":           s.queryCount,
		"query_errors":          s.queryErrors,
		"query_avg_ms":          float64(s.queryTotalTime.Milliseconds()) / max(float64(s.queryCount), 1),
		"query_ops_per_sec":     float64(s.queryCount) / totalElapsed,
		"total_operations":      s.writeCount + s.updateCount + s.deleteCount + s.queryCount,
		"total_errors":          s.writeErrors + s.updateErrors + s.deleteErrors + s.queryErrors,
		"memory_alloc_mb":       float64(m.Alloc) / 1024 / 1024,
		"memory_total_alloc_mb": float64(m.TotalAlloc) / 1024 / 1024,
		"memory_sys_mb":         float64(m.Sys) / 1024 / 1024,
		"num_gc":                m.NumGC,
	}
}

// BenchmarkConfig holds configuration for the long-running benchmark
type BenchmarkConfig struct {
	Duration         time.Duration // How long to run the benchmark
	ReportInterval   time.Duration // How often to print stats
	WriteInterval    time.Duration // How often to write new entities
	UpdateInterval   time.Duration // How often to update existing entities
	DeleteInterval   time.Duration // How often to delete entities
	QueryInterval    time.Duration // How often to query entities
	BatchSize        int           // Number of entities per batch operation
	DeletePercentage float64       // Percentage of entities to delete when deleting
}

// DefaultBenchmarkConfig returns a default configuration
func DefaultBenchmarkConfig() BenchmarkConfig {
	return BenchmarkConfig{
		Duration:         5 * time.Minute,
		ReportInterval:   10 * time.Second,
		WriteInterval:    100 * time.Millisecond,
		UpdateInterval:   150 * time.Millisecond,
		DeleteInterval:   500 * time.Millisecond,
		QueryInterval:    50 * time.Millisecond,
		BatchSize:        10,
		DeletePercentage: 0.1,
	}
}

// EntityPool manages a pool of entity UIDs for operations
type EntityPool struct {
	mu   sync.RWMutex
	uids []string
}

// Add adds UIDs to the pool
func (p *EntityPool) Add(uids ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.uids = append(p.uids, uids...)
}

// GetRandom returns a random UID from the pool
func (p *EntityPool) GetRandom() (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.uids) == 0 {
		return "", false
	}
	return p.uids[rand.Intn(len(p.uids))], true // nolint:gosec // G404: math/rand is sufficient for test data
}

// GetRandomBatch returns a batch of random UIDs
func (p *EntityPool) GetRandomBatch(n int) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.uids) == 0 {
		return nil
	}
	if n > len(p.uids) {
		n = len(p.uids)
	}

	// Create a random sample without replacement
	indices := rand.Perm(len(p.uids))[:n] // nolint:gosec // G404: math/rand is sufficient for test data
	result := make([]string, n)
	for i, idx := range indices {
		result[i] = p.uids[idx]
	}
	return result
}

// Remove removes UIDs from the pool
func (p *EntityPool) Remove(uidsToRemove []string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	removeMap := make(map[string]bool)
	for _, uid := range uidsToRemove {
		removeMap[uid] = true
	}

	filtered := make([]string, 0, len(p.uids))
	for _, uid := range p.uids {
		if !removeMap[uid] {
			filtered = append(filtered, uid)
		}
	}
	p.uids = filtered
}

// Size returns the number of UIDs in the pool
func (p *EntityPool) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.uids)
}

// generateEntity creates a random test entity
// nolint:gosec // G404: math/rand is sufficient for generating test data
func generateEntity(id int) *BenchmarkEntity {
	tags := []string{"tag1", "tag2", "tag3", "tag4", "tag5"}
	numTags := rand.Intn(3) + 1
	selectedTags := make([]string, numTags)
	for i := 0; i < numTags; i++ {
		selectedTags[i] = tags[rand.Intn(len(tags))]
	}

	return &BenchmarkEntity{
		Name:        fmt.Sprintf("Entity-%d", id),
		Description: fmt.Sprintf("Test entity number %d for benchmarking", id),
		Value:       rand.Intn(1000),
		Score:       rand.Float64() * 100,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Tags:        selectedTags,
	}
}

// TestLongRunningBenchmark runs a long-running benchmark test
// This test can be skipped in short mode: go test -short
func TestLongRunningBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running benchmark in short mode")
	}

	// Allow override from environment variable
	durationStr := os.Getenv("BENCHMARK_DURATION")
	config := DefaultBenchmarkConfig()
	if durationStr != "" {
		if duration, err := time.ParseDuration(durationStr); err == nil {
			config.Duration = duration
		}
	}

	runLongRunningBenchmark(t, config)
}

// runLongRunningBenchmark executes the actual benchmark logic
func runLongRunningBenchmark(t *testing.T, config BenchmarkConfig) {
	stdLogger := log.New(os.Stdout, "", log.LstdFlags)
	logger := stdr.NewWithOptions(stdLogger, stdr.Options{LogCaller: stdr.All}).WithName("benchmark")

	// Create client using file-based URI
	tempDir := t.TempDir()
	uri := "file://" + tempDir
	client, err := dgdao.NewClient(
		uri,
		dgdao.WithAutoSchema(false),
		dgdao.WithLogger(logger),
		dgdao.WithCacheSizeMB(128),
	)
	require.NoError(t, err)
	defer client.Close()

	// Context for canceling all operations
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// WaitGroup to track in-flight operations
	var wg sync.WaitGroup

	// Update schema for our benchmark entity
	err = client.UpdateSchema(ctx, BenchmarkEntity{})
	require.NoError(t, err)

	stats := &OperationStats{
		lastReportTime: time.Now(),
		startTime:      time.Now(),
	}
	pool := &EntityPool{uids: make([]string, 0)}
	var entityCounter atomic.Int64
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) // nolint:gosec // G404: math/rand is sufficient for test data
	var rngMu sync.Mutex                                   // Protects rng from concurrent access

	// Setup tickers for different operations
	writeTicker := time.NewTicker(config.WriteInterval)
	updateTicker := time.NewTicker(config.UpdateInterval)
	deleteTicker := time.NewTicker(config.DeleteInterval)
	queryTicker := time.NewTicker(config.QueryInterval)
	reportTicker := time.NewTicker(config.ReportInterval)

	defer writeTicker.Stop()
	defer updateTicker.Stop()
	defer deleteTicker.Stop()
	defer queryTicker.Stop()
	defer reportTicker.Stop()

	timeout := time.After(config.Duration)

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	t.Logf("Starting long-running benchmark for %s", config.Duration)
	t.Logf("Configuration: WriteInterval=%s, UpdateInterval=%s, DeleteInterval=%s, QueryInterval=%s",
		config.WriteInterval, config.UpdateInterval, config.DeleteInterval, config.QueryInterval)

	stats.ResetTimer()

	// Main benchmark loop
	for {
		select {
		case <-sigChan:
			// Stop all tickers to prevent new operations
			writeTicker.Stop()
			updateTicker.Stop()
			deleteTicker.Stop()
			queryTicker.Stop()
			reportTicker.Stop()

			t.Log("\n\nReceived interrupt signal, shutting down gracefully...")
			t.Log("Canceling all operations and waiting for in-flight operations to complete...")
			cancel()  // Signal all goroutines to stop
			wg.Wait() // Wait for all operations to complete

			t.Log(stats.Report())
			finalStats := stats.GetFinalStats()
			if jsonData, err := json.MarshalIndent(finalStats, "", "  "); err == nil {
				t.Logf("\nFinal Statistics (JSON):\n%s", string(jsonData))
				saveStatsToFile(t, finalStats)
			}
			t.Fatal("Test interrupted by user")
			return

		case <-timeout:
			// Stop all tickers to prevent new operations
			writeTicker.Stop()
			updateTicker.Stop()
			deleteTicker.Stop()
			queryTicker.Stop()
			reportTicker.Stop()

			// Cancel context and wait for operations to complete
			t.Log("\nBenchmark completed! Canceling all operations and waiting for in-flight operations...")
			cancel()  // Signal all goroutines to stop
			wg.Wait() // Wait for all operations to complete

			t.Log(stats.Report())

			// Write final stats to JSON for analysis
			finalStats := stats.GetFinalStats()

			if jsonData, err := json.MarshalIndent(finalStats, "", "  "); err == nil {
				t.Logf("\nFinal Statistics (JSON):\n%s", string(jsonData))
				saveStatsToFile(t, finalStats)
			}
			return

		case <-writeTicker.C:
			// Write a batch of new entities
			wg.Add(1)
			go func() {
				defer wg.Done()
				if ctx.Err() != nil {
					return // Context canceled, skip operation
				}
				entities := make([]*BenchmarkEntity, config.BatchSize)
				for i := 0; i < config.BatchSize; i++ {
					id := int(entityCounter.Add(1) - 1)
					entities[i] = generateEntity(id)
				}

				start := time.Now()
				err := client.Insert(ctx, entities)
				duration := time.Since(start)

				// Check for context cancellation (not a real error)
				isContextCanceled := err != nil && ctx.Err() != nil

				if err == nil {
					uids := make([]string, len(entities))
					for i, e := range entities {
						uids[i] = e.UID
					}
					pool.Add(uids...)
				} else if !isContextCanceled {
					t.Logf("Write error (batch size %d, duration %v): %v", len(entities), duration, err)
				}
				stats.RecordWrite(duration, err == nil || isContextCanceled, len(entities))
			}()

		case <-updateTicker.C:
			// Update random entities
			if pool.Size() > 0 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if ctx.Err() != nil {
						return // Context canceled, skip operation
					}
					uids := pool.GetRandomBatch(config.BatchSize)
					if len(uids) == 0 {
						return
					}

					// Fetch entities first
					entities := make([]*BenchmarkEntity, 0, len(uids))
					for _, uid := range uids {
						var entity BenchmarkEntity
						if err := client.Get(ctx, &entity, uid); err == nil {
							entity.Description = fmt.Sprintf("Updated at %s", time.Now().Format(time.RFC3339))
							entity.UpdatedAt = time.Now()
							rngMu.Lock()
							entity.Value = rng.Intn(1000)
							entity.Score = rng.Float64() * 100
							rngMu.Unlock()
							entities = append(entities, &entity)
						}
					}

					if len(entities) > 0 {
						start := time.Now()
						err := client.Update(ctx, entities)
						duration := time.Since(start)

						if err != nil {
							t.Logf("Update error (batch size %d, duration %v): %v", len(entities), duration, err)
						}
						stats.RecordUpdate(duration, err == nil)
					}
				}()
			}

		case <-deleteTicker.C:
			// Delete a small percentage of entities
			if pool.Size() > 0 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if ctx.Err() != nil {
						return // Context canceled, skip operation
					}
					deleteCount := int(float64(pool.Size()) * config.DeletePercentage)
					if deleteCount < 1 {
						deleteCount = 1
					}
					if deleteCount > config.BatchSize {
						deleteCount = config.BatchSize
					}

					uids := pool.GetRandomBatch(deleteCount)
					if len(uids) == 0 {
						return
					}

					start := time.Now()
					err := client.Delete(ctx, uids)
					duration := time.Since(start)

					if err == nil {
						pool.Remove(uids)
					} else {
						t.Logf("Delete error (batch size %d, duration %v): %v", len(uids), duration, err)
					}
					stats.RecordDelete(duration, err == nil, len(uids))
				}()
			}

		case <-queryTicker.C:
			// Perform various queries
			wg.Add(1)
			go func() {
				defer wg.Done()
				if ctx.Err() != nil {
					return // Context canceled, skip operation
				}
				rngMu.Lock()
				queryType := rng.Intn(4)
				rngMu.Unlock()
				start := time.Now()
				var err error

				switch queryType {
				case 0:
					// Query all entities
					var results []BenchmarkEntity
					err = client.Query(ctx, BenchmarkEntity{}).First(100).Nodes(&results)
				case 1:
					// Query by value range
					var results []BenchmarkEntity
					rngMu.Lock()
					minVal := rng.Intn(500)
					maxVal := rng.Intn(500) + 500
					rngMu.Unlock()
					err = client.Query(ctx, BenchmarkEntity{}).
						Filter(fmt.Sprintf("ge(value, %d) AND le(value, %d)", minVal, maxVal)).
						Nodes(&results)
				case 2:
					// Query by tag
					tags := []string{"tag1", "tag2", "tag3", "tag4", "tag5"}
					rngMu.Lock()
					tag := tags[rng.Intn(len(tags))]
					rngMu.Unlock()
					var results []BenchmarkEntity
					err = client.Query(ctx, BenchmarkEntity{}).
						Filter(fmt.Sprintf("anyofterms(tags, \"%s\")", tag)).
						Nodes(&results)
				case 3:
					// Get a specific entity by UID
					if uid, ok := pool.GetRandom(); ok {
						var entity BenchmarkEntity
						err = client.Get(ctx, &entity, uid)
						// If entity not found, it may have been deleted - not an error
						if err != nil && strings.Contains(err.Error(), "not found") {
							err = nil
						}
					}
				}

				duration := time.Since(start)
				isContextCanceled := err != nil && ctx.Err() != nil

				if err != nil && !isContextCanceled {
					t.Logf("Query error (type %d, duration %v): %v", queryType, duration, err)
				}
				stats.RecordQuery(duration, err == nil || isContextCanceled)
			}()

		case <-reportTicker.C:
			// Print periodic stats
			t.Log(stats.Report())
			stats.ResetTimer()
		}
	}
}

// saveStatsToFile saves benchmark statistics to a JSON file
func saveStatsToFile(t *testing.T, stats map[string]interface{}) {
	filename := fmt.Sprintf("benchmark_results_%s.json", time.Now().Format("20060102_150405"))
	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		t.Logf("Warning: Failed to marshal stats: %v", err)
		return
	}
	if err := os.WriteFile(filename, data, 0600); err != nil {
		t.Logf("Warning: Failed to write stats file: %v", err)
	} else {
		t.Logf("\nBenchmark results saved to: %s", filename)
	}
}

// BenchmarkLongRunningLoad is a standard Go benchmark that runs for a shorter duration
func BenchmarkLongRunningLoad(b *testing.B) {
	config := DefaultBenchmarkConfig()
	config.Duration = 1 * time.Minute // Shorter for benchmark mode
	config.ReportInterval = 15 * time.Second

	// Convert to test for reusing the logic
	runLongRunningBenchmark(&testing.T{}, config)
}
