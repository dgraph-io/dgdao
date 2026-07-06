# Load Testing and Benchmarking for dgdao

This directory contains load tests and benchmarks for dgdao.

## Long-Running Benchmark

The `TestLongRunningBenchmark` provides a comprehensive stress test that performs periodic write,
update, delete, and query operations on dgdao while tracking performance metrics.

### Long-Running Benchmark Features

- **Concurrent Operations**: Simulates realistic workload with concurrent writes, updates, deletes,
  and queries
- **Performance Metrics**: Tracks operation counts, average latencies, error rates, and throughput
- **Memory Tracking**: Monitors memory allocation, total allocation, system memory, and GC cycles
- **Configurable Duration**: Can run for any duration via environment variable
- **Periodic Reporting**: Prints performance stats at regular intervals
- **JSON Export**: Saves final statistics to a timestamped JSON file for analysis
- **Graceful Shutdown**: Handles Ctrl+C (SIGINT) and SIGTERM signals to save results before exit
- **Atomic Counters**: Thread-safe entity tracking prevents race conditions

### Running the Long-Running Benchmark

#### Quick Test (5 seconds)

```bash
BENCHMARK_DURATION=5s go test -v -run TestLongRunningBenchmark ./load_test -timeout 30s
```

#### Standard Test (5 minutes - default)

```bash
go test -v -run TestLongRunningBenchmark ./load_test -timeout 10m
```

#### Extended Test (30 minutes)

```bash
BENCHMARK_DURATION=30m go test -v -run TestLongRunningBenchmark ./load_test -timeout 40m
```

#### Skip in Short Mode

```bash
go test -short ./load_test  # Skips long-running benchmark
```

#### Graceful Shutdown

Press **Ctrl+C** during the test to trigger graceful shutdown. The benchmark will:

- Stop all operations immediately
- Print final performance report
- Save results to JSON file
- Exit cleanly

### Long-Running Benchmark Configuration

The benchmark can be configured by modifying `DefaultBenchmarkConfig()` in the code:

- **Duration**: Total runtime of the benchmark (default: 5 minutes)
- **ReportInterval**: How often to print stats (default: 10 seconds)
- **WriteInterval**: How often to write new entities (default: 100ms)
- **UpdateInterval**: How often to update entities (default: 150ms)
- **DeleteInterval**: How often to delete entities (default: 500ms)
- **QueryInterval**: How often to query entities (default: 50ms)
- **BatchSize**: Number of entities per batch operation (default: 10)
- **DeletePercentage**: Percentage of entities to delete (default: 0.1 = 10%)

### Long-Running Benchmark Operations

1. **Writes**: Batch inserts of new entities with random data
2. **Updates**: Fetches random entities and updates their fields
3. **Deletes**: Removes a random percentage of entities
4. **Queries**: Four types of queries:
   - Query all entities (limited to 100)
   - Query by value range
   - Query by tag
   - Get specific entity by UID

### Metrics Tracked

For each operation type:

- Total count
- Error count
- Average latency
- Operations per second

### Long-Running Benchmark Sample Output

```text
=== Performance Report (10.0s interval, 30.0s total) ===
Entities in DB: 408 | Memory: Alloc=295.9MB, TotalAlloc=1382.1MB, Sys=709.3MB

Writes:     98 ops | Avg: 6.123ms | Errors:   0 | Rate: 9.80 ops/s
Updates:    66 ops | Avg: 4.567ms | Errors:   0 | Rate: 6.60 ops/s
Deletes:    18 ops | Avg: 2.134ms | Errors:   0 | Rate: 1.80 ops/s
Queries:   198 ops | Avg: 2.234ms | Errors:   0 | Rate: 19.80 ops/s
========================================
```

**JSON Output** (saved to `benchmark_results_YYYYMMDD_HHMMSS.json`):

```json
{
  "duration_seconds": 30.0,
  "entities_in_db": 408,
  "write_count": 98,
  "write_avg_ms": 6.123,
  "write_ops_per_sec": 9.8,
  "query_count": 198,
  "query_avg_ms": 2.234,
  "total_operations": 380,
  "total_errors": 0,
  "memory_alloc_mb": 295.9,
  "num_gc": 10
}
```

## Unthrottled Benchmark

The `TestDgdaoUnthrottledBenchmark` runs a maximum throughput stress test using concurrent workers
that operate continuously without throttling. This test is designed to measure peak performance and
identify bottlenecks under heavy load.

### Key Differences from Long-Running Benchmark

- **No Throttling**: Workers run continuously in tight loops instead of using tickers
- **Higher Concurrency**: Multiple concurrent workers for each operation type
- **Maximum Throughput**: Tests the absolute limits of the system
- **Worker-Based**: Uses dedicated goroutines for each operation type

### Unthrottled Benchmark Features

- **Concurrent Workers**: Separate worker pools for writes, updates, deletes, and queries
- **Continuous Load**: Each worker operates as fast as possible without artificial delays
- **Performance Metrics**: Same comprehensive metrics as throttled benchmark
- **Graceful Shutdown**: Properly stops all workers and waits for in-flight operations
- **Cancellation Handling**: Distinguishes between real errors and expected shutdown errors

### Running the Unthrottled Benchmark

#### Quick Test (30 seconds)

```bash
BENCHMARK_DURATION=30s go test -v -run=TestDgdaoUnthrottledBenchmark ./load_test -timeout=2m
```

#### Standard Test (3 minutes - default)

```bash
go test -v -run=TestDgdaoUnthrottledBenchmark ./load_test -timeout=5m
```

#### Extended Test (10 minutes)

```bash
BENCHMARK_DURATION=10m go test -v -run=TestDgdaoUnthrottledBenchmark ./load_test -timeout=15m
```

#### Using InsertRaw Mode (High-Performance Inserts)

The unthrottled benchmark supports a special `RAW_INSERT` mode that uses the `InsertRaw` function
for write operations. This bypasses unique checks and uses direct mutation to the Dgraph engine
which can result in significantly higher insert throughput:

```bash
RAW_INSERT=1 go test -v -run=TestDgdaoUnthrottledBenchmark ./load_test -timeout=5m
```

**Note**: `InsertRaw` is only available for local (file-based) dgdao instances. When enabled:

- Write operations use `client.InsertRaw()` instead of `client.Insert()`
- UIDs are pre-assigned using Dgraph's blank node format (`_:entity-<id>`)
- Unique constraint checks are skipped for maximum performance
- Other operations (update, delete, query) continue to work normally

### Unthrottled Benchmark Configuration

The benchmark can be configured by modifying `DefaultUnthrottledConfig()` in the code:

- **Duration**: Total runtime of the benchmark (default: 3 minutes)
- **ReportInterval**: How often to print stats (default: 10 seconds)
- **NumWriteWorkers**: Number of concurrent write workers (default: 4)
- **NumUpdateWorkers**: Number of concurrent update workers (default: 4)
- **NumDeleteWorkers**: Number of concurrent delete workers (default: 1)
- **NumQueryWorkers**: Number of concurrent query workers (default: 8)
- **BatchSize**: Number of entities per batch operation (default: 10)
- **DeletePercentage**: Percentage of entities to delete (default: 0.1 = 10%)

### Unthrottled Benchmark Client Configuration

The test uses optimized client settings:

```go
dgdao.WithCacheSizeMB(1024)  // 1GB posting cache
dgdao.WithPoolSize(20)       // 20 connections in pool
```

### Unthrottled Benchmark Operations

Each worker type operates in a continuous loop:

1. **Write Workers**: Continuously insert batches of new entities
2. **Update Workers**: Continuously fetch and update random entities
3. **Delete Workers**: Continuously delete random batches of entities
4. **Query Workers**: Continuously execute various query types

### Unthrottled Benchmark Sample Output

```text
=== Performance Report (10.0s interval, 60.0s total) ===
Entities in DB: 12847 | Memory: Alloc=512.3MB, TotalAlloc=8472.1MB, Sys=1024.7MB

Writes:   2341 ops | Avg: 18.234ms | Errors:   0 | Rate: 234.10 ops/s
Updates:  1876 ops | Avg: 12.456ms | Errors:   0 | Rate: 187.60 ops/s
Deletes:   418 ops | Avg:  8.123ms | Errors:   0 | Rate:  41.80 ops/s
Queries:  8924 ops | Avg:  3.891ms | Errors:   0 | Rate: 892.40 ops/s
========================================
```

**JSON Output** (saved to `dgdao_unthrottled_results_YYYYMMDD_HHMMSS.json`):

```json
{
  "database": "dgdao",
  "benchmark_type": "unthrottled",
  "duration_seconds": 60.0,
  "entities_in_db": 12847,
  "write_count": 2341,
  "write_avg_ms": 18.234,
  "write_ops_per_sec": 234.1,
  "total_operations": 13559,
  "total_errors": 0,
  "memory_alloc_mb": 512.3,
  "num_gc": 45
}
```

### Performance Tips

- Increase worker counts for higher throughput (may increase contention)
- Adjust `WithCacheSizeMB()` based on available memory
- Monitor memory usage - unthrottled tests can be memory-intensive
- Use longer durations to measure sustained performance after warmup

## Other Load Tests

- `TestLiveLoaderSmall`: Tests loading a small dataset
- `TestLiveLoader1Million`: Tests loading 1 million entities (skipped in short mode)
- `BenchmarkDatabaseOperations`: Standard benchmarks for drop/load and query operations
