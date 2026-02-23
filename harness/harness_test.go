package harness

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseResult(t *testing.T) {
	input := `{
		"client": "geth",
		"state_root": "0xabc123",
		"accounts_created": 100,
		"contracts_created": 10,
		"storage_slots": 500,
		"elapsed_ms": 1234,
		"trie_time_ms": 800,
		"db_write_time_ms": 400,
		"peak_memory_bytes": 104857600
	}`

	result, err := parseResult("geth", bytes.NewReader([]byte(input)))
	if err != nil {
		t.Fatalf("parseResult failed: %v", err)
	}

	if result.Client != "geth" {
		t.Errorf("client = %q, want geth", result.Client)
	}
	if result.StateRoot != "0xabc123" {
		t.Errorf("state_root = %q, want 0xabc123", result.StateRoot)
	}
	if result.AccountsCreated != 100 {
		t.Errorf("accounts_created = %d, want 100", result.AccountsCreated)
	}
	if result.ContractsCreated != 10 {
		t.Errorf("contracts_created = %d, want 10", result.ContractsCreated)
	}
	if result.StorageSlots != 500 {
		t.Errorf("storage_slots = %d, want 500", result.StorageSlots)
	}
	if result.ElapsedMs != 1234 {
		t.Errorf("elapsed_ms = %d, want 1234", result.ElapsedMs)
	}
	if result.PeakMemoryBytes != 104857600 {
		t.Errorf("peak_memory_bytes = %d, want 104857600",
			result.PeakMemoryBytes)
	}
}

func TestParseResultFillsClient(t *testing.T) {
	input := `{"state_root": "0xdef"}`

	result, err := parseResult("reth", bytes.NewReader([]byte(input)))
	if err != nil {
		t.Fatalf("parseResult failed: %v", err)
	}

	if result.Client != "reth" {
		t.Errorf("client = %q, want reth", result.Client)
	}
}

func TestParseResultInvalidJSON(t *testing.T) {
	input := `not json at all`
	_, err := parseResult("test", strings.NewReader(input))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}
