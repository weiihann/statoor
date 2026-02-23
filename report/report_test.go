package report

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/weiihann/statoor/harness"
)

func TestGenerateMatchingRoots(t *testing.T) {
	results := []harness.Result{
		{
			Client:           "geth",
			StateRoot:        "0xabc",
			AccountsCreated:  100,
			ContractsCreated: 10,
			StorageSlots:     500,
			ElapsedMs:        1000,
			TrieTimeMs:       600,
			DBWriteTimeMs:    300,
			PeakMemoryBytes:  100 * 1024 * 1024,
			DBSizeBytes:      50 * 1024 * 1024,
		},
		{
			Client:           "reth",
			StateRoot:        "0xabc",
			AccountsCreated:  100,
			ContractsCreated: 10,
			StorageSlots:     500,
			ElapsedMs:        2000,
			TrieTimeMs:       1200,
			DBWriteTimeMs:    600,
			PeakMemoryBytes:  200 * 1024 * 1024,
			DBSizeBytes:      80 * 1024 * 1024,
		},
	}

	var buf bytes.Buffer
	if err := Generate(&buf, results); err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	output := buf.String()

	if !strings.Contains(output, "all match") {
		t.Error("expected 'all match' for matching roots")
	}
	if !strings.Contains(output, "geth") {
		t.Error("expected geth in output")
	}
	if !strings.Contains(output, "reth") {
		t.Error("expected reth in output")
	}
	if !strings.Contains(output, "2.00x") {
		t.Error("expected 2.00x speedup for reth (twice as slow)")
	}
}

func TestGenerateMismatchedRoots(t *testing.T) {
	results := []harness.Result{
		{Client: "geth", StateRoot: "0xabc", ElapsedMs: 100},
		{Client: "reth", StateRoot: "0xdef", ElapsedMs: 200},
	}

	var buf bytes.Buffer
	if err := Generate(&buf, results); err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	output := buf.String()

	if !strings.Contains(output, "MISMATCH") {
		t.Error("expected MISMATCH for different roots")
	}
	if !strings.Contains(output, "0xabc") {
		t.Error("expected geth root in mismatch details")
	}
	if !strings.Contains(output, "0xdef") {
		t.Error("expected reth root in mismatch details")
	}
}

func TestGenerateEmpty(t *testing.T) {
	var buf bytes.Buffer
	err := Generate(&buf, nil)
	if err == nil {
		t.Error("expected error for empty results")
	}
}

func TestGenerateJSON(t *testing.T) {
	results := []harness.Result{
		{Client: "geth", StateRoot: "0xabc", ElapsedMs: 1000},
	}

	var buf bytes.Buffer
	if err := GenerateJSON(&buf, results); err != nil {
		t.Fatalf("GenerateJSON failed: %v", err)
	}

	var parsed []harness.Result
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}

	if len(parsed) != 1 {
		t.Fatalf("expected 1 result, got %d", len(parsed))
	}
	if parsed[0].Client != "geth" {
		t.Errorf("client = %q, want geth", parsed[0].Client)
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input uint64
		want  string
	}{
		{0, "-"},
		{512, "512 B"},
		{1024, "1 KB"},
		{1536, "1.5 KB"},
		{1048576, "1 MB"},
		{1073741824, "1 GB"},
	}

	for _, tt := range tests {
		got := formatBytes(tt.input)
		if got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestFormatMs(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0ms"},
		{500, "500ms"},
		{999, "999ms"},
		{1000, "1.00s"},
		{1500, "1.50s"},
		{60000, "60.00s"},
	}

	for _, tt := range tests {
		got := formatMs(tt.input)
		if got != tt.want {
			t.Errorf("formatMs(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
