// Geth harness reads a JSONL workload from stdin, applies state operations
// using go-ethereum's native state/trie/database layer (Pebble), and outputs
// benchmark results as JSON to stdout.
package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

type operation struct {
	Op      string `json:"op"`
	Address string `json:"address,omitempty"`
	Balance string `json:"balance,omitempty"`
	Nonce   uint64 `json:"nonce,omitempty"`
	Code    string `json:"code,omitempty"`
	Slot    string `json:"slot,omitempty"`
	Value   string `json:"value,omitempty"`
}

type result struct {
	Client           string `json:"client"`
	StateRoot        string `json:"state_root"`
	AccountsCreated  int    `json:"accounts_created"`
	ContractsCreated int    `json:"contracts_created"`
	StorageSlots     int    `json:"storage_slots"`
	ElapsedMs        int64  `json:"elapsed_ms"`
	TrieTimeMs       int64  `json:"trie_time_ms"`
	DBWriteTimeMs    int64  `json:"db_write_time_ms"`
	PeakMemoryBytes  uint64 `json:"peak_memory_bytes"`
}

func main() {
	dbDir := flag.String("db", "", "database directory")
	flag.Parse()

	if *dbDir == "" {
		fatal("--db flag is required")
	}

	start := time.Now()

	// Open Pebble database.
	kvStore, err := pebble.New(*dbDir, 256, 256, "geth-harness/", false)
	if err != nil {
		fatal("open pebble: %v", err)
	}
	defer kvStore.Close()

	// Wrap KV store into full ethdb.Database (adds ancient store).
	db := rawdb.NewDatabase(kvStore)

	// Create trie and state databases.
	tdb := triedb.NewDatabase(db, triedb.HashDefaults)
	sdb := state.NewDatabase(tdb, nil)

	stateDB, err := state.New(types.EmptyRootHash, sdb)
	if err != nil {
		fatal("create statedb: %v", err)
	}

	var (
		accounts  int
		contracts int
		slots     int
	)

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<20)

	for scanner.Scan() {
		var op operation
		if err := json.Unmarshal(scanner.Bytes(), &op); err != nil {
			fatal("decode operation: %v", err)
		}

		switch op.Op {
		case "create_account":
			addr := common.HexToAddress(op.Address)
			stateDB.CreateAccount(addr)

			if op.Balance != "" {
				bal := hexToUint256(op.Balance)
				stateDB.SetBalance(
					addr, bal,
					tracing.BalanceChangeUnspecified,
				)
			}
			if op.Nonce > 0 {
				stateDB.SetNonce(
					addr, op.Nonce,
					tracing.NonceChangeUnspecified,
				)
			}
			accounts++

		case "set_code":
			addr := common.HexToAddress(op.Address)
			code := hexDecode(op.Code)
			stateDB.SetCode(
				addr, code,
				tracing.CodeChangeUnspecified,
			)
			contracts++

		case "set_storage":
			addr := common.HexToAddress(op.Address)
			slot := common.HexToHash(op.Slot)
			value := common.HexToHash(op.Value)
			stateDB.SetState(addr, slot, value)
			slots++

		case "compute_root":
			emitResult(
				stateDB, tdb, start,
				accounts, contracts, slots,
			)
			return

		default:
			fatal("unknown operation: %s", op.Op)
		}
	}

	if err := scanner.Err(); err != nil {
		fatal("read stdin: %v", err)
	}

	fatal("no compute_root operation found")
}

func emitResult(
	stateDB *state.StateDB,
	tdb *triedb.Database,
	start time.Time,
	accounts, contracts, slots int,
) {
	// Commit state changes to trie.
	trieStart := time.Now()
	root, err := stateDB.Commit(0, false, false)
	if err != nil {
		fatal("commit state: %v", err)
	}
	trieMs := time.Since(trieStart).Milliseconds()

	// Persist trie nodes to disk.
	dbStart := time.Now()
	if err := tdb.Commit(root, false); err != nil {
		fatal("commit trie to disk: %v", err)
	}
	dbWriteMs := time.Since(dbStart).Milliseconds()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	r := result{
		Client:           "geth",
		StateRoot:        root.Hex(),
		AccountsCreated:  accounts,
		ContractsCreated: contracts,
		StorageSlots:     slots,
		ElapsedMs:        time.Since(start).Milliseconds(),
		TrieTimeMs:       trieMs,
		DBWriteTimeMs:    dbWriteMs,
		PeakMemoryBytes:  m.Sys,
	}

	if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
		fatal("encode result: %v", err)
	}
}

func hexToUint256(s string) *uint256.Int {
	s = strings.TrimPrefix(s, "0x")
	b, err := hex.DecodeString(s)
	if err != nil {
		fatal("decode hex balance %q: %v", s, err)
	}

	val := new(uint256.Int)
	val.SetBytes(b)
	return val
}

func hexDecode(s string) []byte {
	s = strings.TrimPrefix(s, "0x")
	b, err := hex.DecodeString(s)
	if err != nil {
		fatal("decode hex %q: %v", s, err)
	}
	return b
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "geth-harness: "+format+"\n", args...)
	os.Exit(1)
}
