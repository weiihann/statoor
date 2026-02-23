// Erigon harness reads a JSONL workload from stdin, applies state operations
// using MDBX (Erigon's native key-value store), and outputs benchmark results
// as JSON to stdout. State root is computed via go-ethereum's StackTrie.
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math/bits"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/erigontech/mdbx-go/mdbx"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/holiman/uint256"
)

// MDBX table names matching Erigon's schema.
const (
	tablePlainState = "PlainState"
	tableCode       = "Code"
)

// emptyCodeHash is the Keccak256 hash of empty bytecode.
var emptyCodeHash = crypto.Keccak256Hash(nil)

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

// accountState tracks in-memory state for a single account.
type accountState struct {
	nonce       uint64
	balance     *uint256.Int
	codeHash    common.Hash
	incarnation uint64
}

// storageEntry is a buffered storage write.
type storageEntry struct {
	addr  common.Address
	inc   uint64
	slot  common.Hash
	value common.Hash
}

// codeEntry is a buffered code write.
type codeEntry struct {
	hash common.Hash
	code []byte
}

func main() {
	dbDir := flag.String("db", "", "database directory")
	flag.Parse()

	if *dbDir == "" {
		fatal("--db flag is required")
	}

	start := time.Now()

	env, err := openMDBX(*dbDir)
	if err != nil {
		fatal("open mdbx: %v", err)
	}
	defer env.Close()

	if err := createTables(env); err != nil {
		fatal("create tables: %v", err)
	}

	accounts := make(map[common.Address]*accountState)
	var storageEntries []storageEntry
	var codeEntries []codeEntry

	var (
		numAccounts  int
		numContracts int
		numSlots     int
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
			bal := uint256.NewInt(0)
			if op.Balance != "" {
				bal = hexToUint256(op.Balance)
			}
			accounts[addr] = &accountState{
				nonce:    op.Nonce,
				balance:  bal,
				codeHash: emptyCodeHash,
			}
			numAccounts++

		case "set_code":
			addr := common.HexToAddress(op.Address)
			code := hexDecode(op.Code)
			codeHash := crypto.Keccak256Hash(code)

			acc, ok := accounts[addr]
			if !ok {
				fatal(
					"set_code for unknown account %s",
					op.Address,
				)
			}
			acc.codeHash = codeHash
			acc.incarnation = 1

			codeEntries = append(codeEntries, codeEntry{
				hash: codeHash,
				code: code,
			})
			numContracts++

		case "set_storage":
			addr := common.HexToAddress(op.Address)
			slot := common.HexToHash(op.Slot)
			value := common.HexToHash(op.Value)

			acc, ok := accounts[addr]
			if !ok {
				fatal(
					"set_storage for unknown account %s",
					op.Address,
				)
			}

			storageEntries = append(storageEntries, storageEntry{
				addr:  addr,
				inc:   acc.incarnation,
				slot:  slot,
				value: value,
			})
			numSlots++

		case "compute_root":
			emitResult(
				env, accounts, storageEntries, codeEntries,
				start, numAccounts, numContracts, numSlots,
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
	env *mdbx.Env,
	accounts map[common.Address]*accountState,
	storage []storageEntry,
	code []codeEntry,
	start time.Time,
	numAccounts, numContracts, numSlots int,
) {
	// Compute the state root via StackTrie.
	trieStart := time.Now()
	root := computeStateRoot(accounts, storage)
	trieMs := time.Since(trieStart).Milliseconds()

	// Write all data to MDBX.
	dbStart := time.Now()
	if err := writeMDBX(env, accounts, storage, code); err != nil {
		fatal("write mdbx: %v", err)
	}
	dbWriteMs := time.Since(dbStart).Milliseconds()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	r := result{
		Client:           "erigon",
		StateRoot:        root.Hex(),
		AccountsCreated:  numAccounts,
		ContractsCreated: numContracts,
		StorageSlots:     numSlots,
		ElapsedMs:        time.Since(start).Milliseconds(),
		TrieTimeMs:       trieMs,
		DBWriteTimeMs:    dbWriteMs,
		PeakMemoryBytes:  m.Sys,
	}

	if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
		fatal("encode result: %v", err)
	}
}

// computeStateRoot builds a standard Ethereum MPT state root from the
// accumulated account/storage data using go-ethereum's StackTrie.
func computeStateRoot(
	accounts map[common.Address]*accountState,
	storage []storageEntry,
) common.Hash {
	// Group storage by address for per-account storage root computation.
	storageByAddr := make(
		map[common.Address][]storageEntry, len(accounts),
	)
	for i := range storage {
		addr := storage[i].addr
		storageByAddr[addr] = append(storageByAddr[addr], storage[i])
	}

	// Build sorted list of (addrHash, address) for deterministic
	// StackTrie insertion order.
	type addrWithHash struct {
		addr     common.Address
		addrHash common.Hash
	}
	sorted := make([]addrWithHash, 0, len(accounts))
	for addr := range accounts {
		sorted = append(sorted, addrWithHash{
			addr:     addr,
			addrHash: crypto.Keccak256Hash(addr.Bytes()),
		})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(
			sorted[i].addrHash[:], sorted[j].addrHash[:],
		) < 0
	})

	accountTrie := trie.NewStackTrie(nil)

	for _, item := range sorted {
		acc := accounts[item.addr]

		storageRoot := types.EmptyRootHash
		if slots, ok := storageByAddr[item.addr]; ok && len(slots) > 0 {
			storageRoot = computeStorageRoot(slots)
		}

		stateAcc := types.StateAccount{
			Nonce:    acc.nonce,
			Balance:  acc.balance,
			Root:     storageRoot,
			CodeHash: acc.codeHash.Bytes(),
		}

		data, err := rlp.EncodeToBytes(&stateAcc)
		if err != nil {
			fatal("rlp encode account: %v", err)
		}
		accountTrie.Update(item.addrHash[:], data)
	}

	return accountTrie.Hash()
}

// computeStorageRoot computes the storage trie root for a single account.
func computeStorageRoot(slots []storageEntry) common.Hash {
	type slotWithHash struct {
		slot    common.Hash
		value   common.Hash
		keyHash common.Hash
	}

	hashed := make([]slotWithHash, len(slots))
	for i, s := range slots {
		hashed[i] = slotWithHash{
			slot:    s.slot,
			value:   s.value,
			keyHash: crypto.Keccak256Hash(s.slot.Bytes()),
		}
	}

	sort.Slice(hashed, func(i, j int) bool {
		return bytes.Compare(
			hashed[i].keyHash[:], hashed[j].keyHash[:],
		) < 0
	})

	storageTrie := trie.NewStackTrie(nil)
	for _, h := range hashed {
		trimmed := trimLeftZeroes(h.value[:])
		if len(trimmed) == 0 {
			continue
		}
		encoded, err := rlp.EncodeToBytes(trimmed)
		if err != nil {
			fatal("rlp encode storage value: %v", err)
		}
		storageTrie.Update(h.keyHash[:], encoded)
	}

	return storageTrie.Hash()
}

// openMDBX creates and opens an MDBX environment.
func openMDBX(path string) (*mdbx.Env, error) {
	env, err := mdbx.NewEnv()
	if err != nil {
		return nil, fmt.Errorf("create env: %w", err)
	}

	if err := env.SetOption(mdbx.OptMaxDB, 100); err != nil {
		return nil, fmt.Errorf("set max dbs: %w", err)
	}

	if err := env.SetGeometry(-1, -1, 1<<40, -1, -1, 4096); err != nil {
		return nil, fmt.Errorf("set geometry: %w", err)
	}

	flags := uint(mdbx.NoReadahead | mdbx.Coalesce | mdbx.Durable)
	if err := env.Open(path, flags, 0644); err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	return env, nil
}

// createTables opens the required MDBX named databases (tables).
func createTables(env *mdbx.Env) error {
	return env.Update(func(txn *mdbx.Txn) error {
		if _, err := txn.OpenDBI(
			tablePlainState, mdbx.Create, nil, nil,
		); err != nil {
			return fmt.Errorf("create %s: %w", tablePlainState, err)
		}
		if _, err := txn.OpenDBI(
			tableCode, mdbx.Create, nil, nil,
		); err != nil {
			return fmt.Errorf("create %s: %w", tableCode, err)
		}
		return nil
	})
}

// writeMDBX writes all accounts, storage, and code to MDBX tables.
func writeMDBX(
	env *mdbx.Env,
	accounts map[common.Address]*accountState,
	storage []storageEntry,
	code []codeEntry,
) error {
	return env.Update(func(txn *mdbx.Txn) error {
		plainDBI, err := txn.OpenDBI(tablePlainState, 0, nil, nil)
		if err != nil {
			return fmt.Errorf("open %s: %w", tablePlainState, err)
		}

		codeDBI, err := txn.OpenDBI(tableCode, 0, nil, nil)
		if err != nil {
			return fmt.Errorf("open %s: %w", tableCode, err)
		}

		for addr, acc := range accounts {
			encoded := encodeAccount(acc)
			if err := txn.Put(
				plainDBI, addr[:], encoded, 0,
			); err != nil {
				return fmt.Errorf(
					"write account %s: %w", addr.Hex(), err,
				)
			}
		}

		for _, s := range storage {
			key := makeStorageKey(s.addr, s.inc, s.slot)
			trimmed := trimLeftZeroes(s.value[:])
			if len(trimmed) == 0 {
				continue
			}
			if err := txn.Put(
				plainDBI, key, trimmed, 0,
			); err != nil {
				return fmt.Errorf(
					"write storage %s/%s: %w",
					s.addr.Hex(), s.slot.Hex(), err,
				)
			}
		}

		for _, c := range code {
			if err := txn.Put(
				codeDBI, c.hash[:], c.code, 0,
			); err != nil {
				return fmt.Errorf(
					"write code %s: %w", c.hash.Hex(), err,
				)
			}
		}

		return nil
	})
}

// makeStorageKey builds the PlainState storage key:
// address(20) + incarnation(8, big-endian) + slot(32).
func makeStorageKey(
	addr common.Address, incarnation uint64, slot common.Hash,
) []byte {
	key := make([]byte, 20+8+32)
	copy(key[:20], addr[:])
	binary.BigEndian.PutUint64(key[20:28], incarnation)
	copy(key[28:], slot[:])
	return key
}

// encodeAccount produces the Erigon SerialiseV3 / fieldset-based encoding:
//
//	byte 0: fieldset (bit0=nonce, bit1=balance, bit2=incarnation, bit3=codeHash)
//	each present field: 1-byte length prefix + big-endian value bytes
func encodeAccount(acc *accountState) []byte {
	var fieldSet byte
	var buf []byte

	if acc.nonce > 0 {
		fieldSet |= 1
		n := bitLenToByteLen(bits.Len64(acc.nonce))
		buf = append(buf, byte(n))
		for i := n; i > 0; i-- {
			buf = append(buf, byte(acc.nonce>>(8*(i-1))))
		}
	}

	if !acc.balance.IsZero() {
		fieldSet |= 2
		n := acc.balance.ByteLen()
		buf = append(buf, byte(n))
		balBytes := make([]byte, n)
		acc.balance.WriteToSlice(balBytes)
		buf = append(buf, balBytes...)
	}

	if acc.incarnation > 0 {
		fieldSet |= 4
		n := bitLenToByteLen(bits.Len64(acc.incarnation))
		buf = append(buf, byte(n))
		for i := n; i > 0; i-- {
			buf = append(buf, byte(acc.incarnation>>(8*(i-1))))
		}
	}

	if acc.codeHash != emptyCodeHash && acc.codeHash != (common.Hash{}) {
		fieldSet |= 8
		buf = append(buf, 32)
		buf = append(buf, acc.codeHash[:]...)
	}

	out := make([]byte, 1+len(buf))
	out[0] = fieldSet
	copy(out[1:], buf)
	return out
}

func bitLenToByteLen(bitLen int) int {
	return (bitLen + 7) / 8
}

func trimLeftZeroes(s []byte) []byte {
	for i, v := range s {
		if v != 0 {
			return s[i:]
		}
	}
	return nil
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
	fmt.Fprintf(os.Stderr, "erigon-harness: "+format+"\n", args...)
	os.Exit(1)
}
