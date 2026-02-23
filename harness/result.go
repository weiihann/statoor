// Package harness manages execution of per-client state benchmark binaries.
package harness

// Result holds the structured output from a harness execution.
type Result struct {
	Client           string `json:"client"`
	StateRoot        string `json:"state_root"`
	AccountsCreated  int    `json:"accounts_created"`
	ContractsCreated int    `json:"contracts_created"`
	StorageSlots     int    `json:"storage_slots"`
	ElapsedMs        int64  `json:"elapsed_ms"`
	TrieTimeMs       int64  `json:"trie_time_ms"`
	DBWriteTimeMs    int64  `json:"db_write_time_ms"`
	PeakMemoryBytes  uint64 `json:"peak_memory_bytes"`
	DBSizeBytes      uint64 `json:"db_size_bytes"`
}
