# dgdao Query CLI

This command-line tool allows you to run arbitrary DQL (Dgraph Query Language) queries against a
dgdao database, either in local file-based mode or (optionally) against a remote Dgraph-compatible
endpoint.

## Requirements

- Go 1.24 or higher
- Access to a directory containing a dgdao database (created by dgdao)

## Installation

```bash
# Navigate to the cmd/query directory
cd cmd/query

# Run directly
go run main.go --dir /path/to/dgdao [options]

# Or build and then run
go build -o dgdao-query
./dgdao-query --dir /path/to/dgdao [options]
```

## Usage

The tool reads a DQL query from standard input and prints the JSON response to standard output.

```sh
Usage of ./main:
  --dir string     Directory where the dgdao database is stored (required)
  --pretty         Pretty-print the JSON output (default true)
  --timeout        Query timeout duration (default 30s)
  -v int           Verbosity level for logging (e.g., -v=1, -v=2)
```

### Example: Querying the Graph

```bash
echo '{ q(func: has(name@en), first: 10) { id: uid name@en } }' | go run main.go --dir /tmp/dgdao
```

### Example: With Verbose Logging

```bash
echo '{ q(func: has(name@en), first: 10) { id: uid name@en } }' | go run main.go --dir /tmp/dgdao -v 1
```

### Example: Build and Run

```bash
go build -o dgdao-query
cat query.dql | ./dgdao-query --dir /tmp/dgdao
```

## Notes

- The `--dir` flag is required and must point to a directory initialized by dgdao.
- The query must be provided via standard input.
- Use the `-v` flag to control logging verbosity (higher values show more log output).
- Use the `--pretty=false` flag to disable pretty-printing of the JSON response.
- The tool logs query timing and errors to standard error.

## Example Output

```json
{
  "q": [
    { "id": "0x2", "name@en": "Ivan Sen" },
    { "id": "0x3", "name@en": "Peter Lord" }
  ]
}
```

---

For more advanced usage and integration, see the main [dgdao documentation](../../README.md).
