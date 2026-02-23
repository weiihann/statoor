# statoor

Cross-client Ethereum state benchmarking tool. Runs the same deterministic workload through each client's native state/trie/database layer and compares performance.

## Supported clients

| Client | Language | Database | Trie |
|--------|----------|----------|------|
| geth | Go | Pebble | StateDB + TrieDB |
| erigon | Go | MDBX | PlainState + StackTrie |
| reth | Rust | MDBX | HashedAccounts + StateRoot |
| ethrex | Rust | RocksDB | In-memory Store trie |
| besu | Java | In-memory KV | Forest world state |
| nethermind | C# | RocksDB | WorldState + TrieStore |

## Architecture

```
statoor run --clients geth,reth,erigon --accounts 10000 --seed 42
         |
         v
  +--------------+
  |   Workload   |     Generate deterministic JSONL
  |   Generator  |     (create_account, set_code, set_storage, compute_root)
  +--------------+
         |
         v
  +--------------+     Spawn each harness as a subprocess
  |   Harness    |     Pipe workload via stdin
  |   Runner     |     Capture JSON result from stdout
  +--------------+
         |
         v
  +--------------+
  |   Report     |     Compare state roots, show timings
  +--------------+
```

Each harness is a standalone binary that imports the client's actual state layer. The orchestrator generates a workload, pipes it to each harness, and collects results.

## Quick start

Build the orchestrator:

```bash
make build
```

Build a harness (example: geth):

```bash
cd harnesses/geth && go build -o geth-harness .
```

Run a benchmark:

```bash
./bin/statoor run \
  --clients geth \
  --accounts 1000 \
  --contracts 100 \
  --seed 42 \
  --skip-build
```

Run all 6 clients:

```bash
./bin/statoor run \
  --clients geth,erigon,reth,ethrex,besu,nethermind \
  --accounts 10000 \
  --contracts 1000 \
  --seed 42 \
  --skip-build
```

## Building harnesses

Each harness has its own build system:

```bash
# Go clients
cd harnesses/geth && go build -o geth-harness .
cd harnesses/erigon && go build -o erigon-harness .

# Rust clients
cd harnesses/reth && cargo build --release
cd harnesses/ethrex && cargo build --release

# Java (requires JDK 21+)
cd harnesses/besu && ./gradlew shadowJar

# C# (requires .NET 10)
cd harnesses/nethermind && dotnet build -c Release
```

Or let the orchestrator build automatically (omit `--skip-build`):

```bash
./bin/statoor run --clients geth,reth --accounts 1000 --seed 42
```

## CLI flags

```
--accounts      Number of EOA accounts (default: 1000)
--contracts     Number of contracts (default: 100)
--max-slots     Maximum storage slots per contract (default: 10000)
--min-slots     Minimum storage slots per contract (default: 1)
--distribution  Slot distribution: power-law, uniform, exponential (default: power-law)
--seed          Random seed, 0 = current time (default: 0)
--code-size     Average contract code size in bytes (default: 1024)
--clients       Comma-separated client list (required)
--db-dir        Base directory for databases (default: temp dir)
--workload      Path to pre-generated JSONL workload (skip generation)
--harnesses-dir Path to harnesses directory (default: ./harnesses)
--skip-build    Skip building harness binaries
--json          Output results as JSON instead of table
```

## Workload format

The workload is a JSONL file where each line is one operation:

```jsonl
{"op":"create_account","address":"0x...","balance":"0x...","nonce":42}
{"op":"set_code","address":"0x...","code":"0x..."}
{"op":"set_storage","address":"0x...","slot":"0x...","value":"0x..."}
{"op":"compute_root"}
```

Operations:
- `create_account` — Create an account with balance and nonce
- `set_code` — Deploy bytecode to an address (must follow create_account)
- `set_storage` — Set a storage slot on an address
- `compute_root` — Flush writes, compute state root, emit results (must be last)

## Output

Markdown table (default):

```
## Benchmark Results

State roots: all match

| Client     | Elapsed | Trie Time | DB Write | Peak Mem | DB Size | Speedup |
|------------|---------|-----------|----------|----------|---------|---------|
| geth       | 2ms     | 1ms       | 0ms      | 19 MB    | 28 KB   | 2.00x   |
| erigon     | 0ms     | 0ms       | 0ms      | 8 MB     | 64 KB   | 1.00x   |
| reth       | 1ms     | 0ms       | 0ms      | 5 MB     | 4 GB    | 1.00x   |
| ethrex     | 5ms     | 0ms       | 0ms      | 889 MB   | 309 KB  | 5.00x   |
| besu       | 170ms   | 5ms       | 9ms      | 53 MB    | -       | 170.00x |
| nethermind | 303ms   | 43ms      | 33ms     | 123 MB   | 63 KB   | 303.00x |
```

JSON output (`--json`):

```json
[
  {
    "client": "geth",
    "state_root": "0x3c6158...",
    "accounts_created": 110,
    "contracts_created": 10,
    "storage_slots": 12,
    "elapsed_ms": 2,
    "trie_time_ms": 1,
    "db_write_time_ms": 0,
    "peak_memory_bytes": 20272392,
    "db_size_bytes": 28396
  }
]
```

## Project structure

```
cmd/statoor/main.go      CLI entry point and benchmark pipeline
workload/                 Deterministic JSONL workload generation
harness/                  Harness process runner and build logic
report/                   Result comparison and formatting
harnesses/
  geth/                   Go — Pebble + StateDB
  erigon/                 Go — MDBX + StackTrie
  reth/                   Rust — MDBX + reth-trie
  ethrex/                 Rust — RocksDB + ethrex-trie
  besu/                   Java — In-memory KV + Forest trie
  nethermind/             C# — RocksDB + Patricia trie
```

## Prerequisites

Building all 6 harnesses requires:

- Go 1.24+
- Rust (latest stable)
- JDK 21+
- .NET 10

Each harness depends on its client's source tree via local path references. The expected layout:

```
/mnt/disk0/vibecode/statoor/
  statoor/          # This repo
  reth/             # github.com/paradigmxyz/reth
  ethrex/           # github.com/lambdaclass/ethrex
```

Geth and erigon harnesses use `go.sum`-pinned versions. Besu uses Maven artifacts.

## Testing

```bash
make test    # go test -race ./...
make lint    # go vet ./...
```
