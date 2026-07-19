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

// isCancellationError checks if an error is due to context cancellation
func isCancellationError(err error) bool {
	if err == nil {
		return false
	}
	// Check for context errors
	if err == context.Canceled || err == context.DeadlineExceeded {
		return true
	}
	// Check error message for cancellation indicators
	errMsg := err.Error()
	return strings.Contains(errMsg, "context canceled") ||
		strings.Contains(errMsg, "context deadline exceeded") ||
		strings.Contains(errMsg, "operation was canceled")
}

// UnthrottledConfig holds configuration for unthrottled benchmarks
type UnthrottledConfig struct {
	Duration         time.Duration // How long to run the benchmark
	ReportInterval   time.Duration // How often to print stats
	NumWriteWorkers  int           // Number of concurrent write workers
	NumUpdateWorkers int           // Number of concurrent update workers
	NumDeleteWorkers int           // Number of concurrent delete workers
	NumQueryWorkers  int           // Number of concurrent query workers
	BatchSize        int           // Number of entities per batch operation
	DeletePercentage float64       // Percentage of entities to delete when deleting
}

// DefaultUnthrottledConfig returns a default unthrottled configuration
func DefaultUnthrottledConfig() UnthrottledConfig {
	return UnthrottledConfig{
		Duration:         3 * time.Minute,
		ReportInterval:   10 * time.Second,
		NumWriteWorkers:  4,  // 4 concurrent writers
		NumUpdateWorkers: 4,  // 4 concurrent updaters
		NumDeleteWorkers: 1,  // 1 concurrent deleter
		NumQueryWorkers:  8,  // 8 concurrent query workers
		BatchSize:        10, // 50 entities per batch
		DeletePercentage: 0.1,
	}
}

// TestDgdaoUnthrottledBenchmark runs an unthrottled benchmark test
func TestDgdaoUnthrottledBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping unthrottled benchmark in short mode")
	}

	config := DefaultUnthrottledConfig()
	if durationStr := os.Getenv("BENCHMARK_DURATION"); durationStr != "" {
		if duration, err := time.ParseDuration(durationStr); err == nil {
			config.Duration = duration
		}
	}

	runDgdaoUnthrottled(t, config)
}

func runDgdaoUnthrottled(t *testing.T, config UnthrottledConfig) {
	stdLogger := log.New(os.Stdout, "", log.LstdFlags)
	logger := stdr.NewWithOptions(stdLogger, stdr.Options{LogCaller: stdr.All}).WithName("unthrottled-benchmark")

	tempDir := t.TempDir()
	uri := "file://" + tempDir
	client, err := dgdao.NewClient(
		uri,
		dgdao.WithAutoSchema(false),
		dgdao.WithLogger(logger),
		dgdao.WithCacheSizeMB(2048),
		dgdao.WithPoolSize(50),
	)
	require.NoError(t, err)
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = client.UpdateSchema(ctx, BenchmarkEntity{})
	require.NoError(t, err)

	stats := &OperationStats{
		lastReportTime: time.Now(),
		startTime:      time.Now(),
	}
	pool := &EntityPool{uids: make([]string, 0)}
	entityCounter := atomic.Int64{}

	var wg sync.WaitGroup
	stopChan := make(chan struct{})
	reportTicker := time.NewTicker(config.ReportInterval)
	defer reportTicker.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	t.Logf("Starting dgdao unthrottled benchmark for %s", config.Duration)
	t.Logf("Workers: Write=%d, Update=%d, Delete=%d, Query=%d",
		config.NumWriteWorkers, config.NumUpdateWorkers, config.NumDeleteWorkers, config.NumQueryWorkers)

	// Start write workers
	for i := 0; i < config.NumWriteWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// nolint:gosec // G404: math/rand is sufficient for test data
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			for {
				select {
				case <-stopChan:
					return
				case <-ctx.Done():
					return
				default:
					entities := make([]*BenchmarkEntity, config.BatchSize)
					for i := 0; i < config.BatchSize; i++ {
						id := int(entityCounter.Add(1))
						entities[i] = generateEntity(id)
					}

					start := time.Now()
					err := client.Insert(ctx, entities)
					duration := time.Since(start)

					if err == nil {
						uids := make([]string, len(entities))
						for i, e := range entities {
							uids[i] = e.UID
						}
						pool.Add(uids...)
					} else if !isCancellationError(err) {
						// Only log non-cancellation errors
						t.Logf("[Write Worker %d] Error inserting batch of %d (duration %v): %v", workerID, len(entities), duration, err)
					}
					// Treat cancellation errors as success for stats (expected during shutdown)
					stats.RecordWrite(duration, err == nil || isCancellationError(err), len(entities))
					_ = rng // keep linter happy
				}
			}
		}(i)
	}

	// Start update workers
	for i := 0; i < config.NumUpdateWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// nolint:gosec // G404: math/rand is sufficient for test data
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			for {
				select {
				case <-stopChan:
					return
				case <-ctx.Done():
					return
				default:
					if pool.Size() == 0 {
						time.Sleep(10 * time.Millisecond)
						continue
					}

					uids := pool.GetRandomBatch(config.BatchSize)
					if len(uids) == 0 {
						continue
					}

					entities := make([]*BenchmarkEntity, 0, len(uids))
					for _, uid := range uids {
						var entity BenchmarkEntity
						if err := client.Get(ctx, &entity, uid); err == nil {
							entity.Description = fmt.Sprintf("Updated at %s", time.Now().Format(time.RFC3339))
							entity.UpdatedAt = time.Now()
							entity.Value = rng.Intn(1000)
							entity.Score = rng.Float64() * 100
							entities = append(entities, &entity)
						}
					}

					if len(entities) > 0 {
						start := time.Now()
						err := client.Update(ctx, entities)
						duration := time.Since(start)
						if err != nil && !isCancellationError(err) {
							// Only log non-cancellation errors
							t.Logf("[Update Worker %d] Error updating batch of %d (duration %v): %v", workerID, len(entities), duration, err)
						}
						// Treat cancellation errors as success for stats (expected during shutdown)
						stats.RecordUpdate(duration, err == nil || isCancellationError(err))
					}
				}
			}
		}(i)
	}

	// Start delete workers
	for i := 0; i < config.NumDeleteWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				case <-ctx.Done():
					return
				default:
					if pool.Size() == 0 {
						time.Sleep(50 * time.Millisecond)
						continue
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
						continue
					}

					start := time.Now()
					err := client.Delete(ctx, uids)
					duration := time.Since(start)

					if err == nil {
						pool.Remove(uids)
					} else if !isCancellationError(err) {
						// Only log non-cancellation errors
						t.Logf("[Delete Worker %d] Error deleting batch of %d (duration %v): %v", workerID, len(uids), duration, err)
					}
					// Treat cancellation errors as success for stats (expected during shutdown)
					stats.RecordDelete(duration, err == nil || isCancellationError(err), len(uids))
				}
			}
		}(i)
	}

	// Start query workers
	for i := 0; i < config.NumQueryWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// nolint:gosec // G404: math/rand is sufficient for test data
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			for {
				select {
				case <-stopChan:
					return
				case <-ctx.Done():
					return
				default:
					queryType := rng.Intn(4)
					start := time.Now()
					var err error

					switch queryType {
					case 0:
						var results []BenchmarkEntity
						err = client.Query(ctx, BenchmarkEntity{}).First(100).Nodes(&results)
					case 1:
						var results []BenchmarkEntity
						err = client.Query(ctx, BenchmarkEntity{}).
							Filter(fmt.Sprintf("ge(value, %d) AND le(value, %d)", rng.Intn(500), rng.Intn(500)+500)).
							Nodes(&results)
					case 2:
						tags := []string{"tag1", "tag2", "tag3", "tag4", "tag5"}
						tag := tags[rng.Intn(len(tags))]
						var results []BenchmarkEntity
						err = client.Query(ctx, BenchmarkEntity{}).
							Filter(fmt.Sprintf("anyofterms(tags, \"%s\")", tag)).
							Nodes(&results)
					case 3:
						if uid, ok := pool.GetRandom(); ok {
							var entity BenchmarkEntity
							err = client.Get(ctx, &entity, uid)
							// If entity not found, it may have been deleted - not an error for benchmarking
							if err != nil && strings.Contains(err.Error(), "not found") {
								err = nil
							}
						}
					}

					duration := time.Since(start)
					if err != nil && !isCancellationError(err) {
						// Only log non-cancellation errors (not found errors are already handled)
						t.Logf("[Query Worker %d] Error on query type %d (duration %v): %v", workerID, queryType, duration, err)
					}
					// Treat cancellation errors as success for stats (expected during shutdown)
					stats.RecordQuery(duration, err == nil || isCancellationError(err))
				}
			}
		}(i)
	}

	timeout := time.After(config.Duration)

	// Main monitoring loop
	for {
		select {
		case <-sigChan:
			t.Log("\n\nReceived interrupt signal, shutting down gracefully...")
			close(stopChan)
			cancel()
			wg.Wait()

			t.Log(stats.Report())
			finalStats := stats.GetFinalStats()
			if jsonData, err := json.MarshalIndent(finalStats, "", "  "); err == nil {
				t.Logf("\nFinal Statistics (JSON):\n%s", string(jsonData))
				saveUnthrottledStatsToFile(t, finalStats, "dgdao")
			}
			t.Fatal("Test interrupted by user")
			return

		case <-timeout:
			t.Log("\nBenchmark completed! Shutting down workers...")
			close(stopChan)
			cancel()
			wg.Wait()

			t.Log(stats.Report())
			finalStats := stats.GetFinalStats()
			if jsonData, err := json.MarshalIndent(finalStats, "", "  "); err == nil {
				t.Logf("\nFinal Statistics (JSON):\n%s", string(jsonData))
				saveUnthrottledStatsToFile(t, finalStats, "dgdao")
			}
			return

		case <-reportTicker.C:
			t.Log(stats.Report())
			stats.ResetTimer()
		}
	}
}

// saveUnthrottledStatsToFile saves unthrottled benchmark statistics to a JSON file
func saveUnthrottledStatsToFile(t *testing.T, stats map[string]interface{}, dbType string) {
	filename := fmt.Sprintf("%s_unthrottled_results_%s.json", dbType, time.Now().Format("20060102_150405"))
	stats["benchmark_type"] = "unthrottled"
	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		t.Logf("Warning: Failed to marshal stats: %v", err)
		return
	}
	if err := os.WriteFile(filename, data, 0600); err != nil {
		t.Logf("Warning: Failed to write stats file: %v", err)
	} else {
		t.Logf("\nUnthrottled benchmark results saved to: %s", filename)
	}
}
