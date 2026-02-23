package workload

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestGenerateDeterministic(t *testing.T) {
	cfg := Config{
		NumAccounts:  10,
		NumContracts: 5,
		MaxSlots:     20,
		MinSlots:     1,
		Distribution: "uniform",
		Seed:         42,
		CodeSize:     64,
	}

	var buf1, buf2 bytes.Buffer

	gen1 := NewGenerator(cfg)
	sum1, err := gen1.Generate(&buf1)
	if err != nil {
		t.Fatalf("first generation failed: %v", err)
	}

	gen2 := NewGenerator(cfg)
	sum2, err := gen2.Generate(&buf2)
	if err != nil {
		t.Fatalf("second generation failed: %v", err)
	}

	if buf1.String() != buf2.String() {
		t.Error("workloads are not deterministic for same seed")
	}

	if sum1 != sum2 {
		t.Errorf("summaries differ: %+v vs %+v", sum1, sum2)
	}
}

func TestGenerateCounts(t *testing.T) {
	tests := []struct {
		name     string
		cfg      Config
		wantAcct int
		wantCtrt int
	}{
		{
			name: "basic",
			cfg: Config{
				NumAccounts:  5,
				NumContracts: 3,
				MaxSlots:     10,
				MinSlots:     1,
				Distribution: "uniform",
				Seed:         1,
				CodeSize:     32,
			},
			wantAcct: 5,
			wantCtrt: 3,
		},
		{
			name: "no contracts",
			cfg: Config{
				NumAccounts:  10,
				NumContracts: 0,
				MaxSlots:     10,
				MinSlots:     1,
				Distribution: "uniform",
				Seed:         2,
				CodeSize:     32,
			},
			wantAcct: 10,
			wantCtrt: 0,
		},
		{
			name: "no accounts",
			cfg: Config{
				NumAccounts:  0,
				NumContracts: 5,
				MaxSlots:     5,
				MinSlots:     1,
				Distribution: "power-law",
				Seed:         3,
				CodeSize:     32,
			},
			wantAcct: 0,
			wantCtrt: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			gen := NewGenerator(tt.cfg)

			sum, err := gen.Generate(&buf)
			if err != nil {
				t.Fatalf("generation failed: %v", err)
			}

			if sum.AccountsCreated != tt.wantAcct {
				t.Errorf("accounts: got %d, want %d",
					sum.AccountsCreated, tt.wantAcct)
			}
			if sum.ContractsCreated != tt.wantCtrt {
				t.Errorf("contracts: got %d, want %d",
					sum.ContractsCreated, tt.wantCtrt)
			}
		})
	}
}

func TestGenerateValidJSONL(t *testing.T) {
	cfg := Config{
		NumAccounts:  5,
		NumContracts: 3,
		MaxSlots:     5,
		MinSlots:     1,
		Distribution: "uniform",
		Seed:         42,
		CodeSize:     32,
	}

	var buf bytes.Buffer
	gen := NewGenerator(cfg)
	if _, err := gen.Generate(&buf); err != nil {
		t.Fatalf("generation failed: %v", err)
	}

	scanner := bufio.NewScanner(&buf)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		var op Operation
		if err := json.Unmarshal([]byte(line), &op); err != nil {
			t.Errorf("line %d: invalid JSON: %v\nline: %s", lineNum, err, line)

			continue
		}

		switch op.Op {
		case "create_account":
			if !strings.HasPrefix(op.Address, "0x") {
				t.Errorf("line %d: address missing 0x prefix: %s",
					lineNum, op.Address)
			}
		case "set_code":
			if !strings.HasPrefix(op.Code, "0x") {
				t.Errorf("line %d: code missing 0x prefix: %s",
					lineNum, op.Code)
			}
		case "set_storage":
			if !strings.HasPrefix(op.Slot, "0x") {
				t.Errorf("line %d: slot missing 0x prefix: %s",
					lineNum, op.Slot)
			}
			if !strings.HasPrefix(op.Value, "0x") {
				t.Errorf("line %d: value missing 0x prefix: %s",
					lineNum, op.Value)
			}
		case "compute_root":
			// No fields required.
		default:
			t.Errorf("line %d: unknown op %q", lineNum, op.Op)
		}
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner error: %v", err)
	}
}

func TestGenerateLastOpIsComputeRoot(t *testing.T) {
	cfg := Config{
		NumAccounts:  3,
		NumContracts: 2,
		MaxSlots:     3,
		MinSlots:     1,
		Distribution: "uniform",
		Seed:         99,
		CodeSize:     16,
	}

	var buf bytes.Buffer
	gen := NewGenerator(cfg)
	if _, err := gen.Generate(&buf); err != nil {
		t.Fatalf("generation failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 0 {
		t.Fatal("empty output")
	}

	var lastOp Operation
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &lastOp); err != nil {
		t.Fatalf("failed to parse last line: %v", err)
	}

	if lastOp.Op != "compute_root" {
		t.Errorf("last op = %q, want compute_root", lastOp.Op)
	}
}

func TestDistributions(t *testing.T) {
	for _, dist := range []string{"power-law", "exponential", "uniform"} {
		t.Run(dist, func(t *testing.T) {
			cfg := Config{
				NumAccounts:  0,
				NumContracts: 100,
				MaxSlots:     1000,
				MinSlots:     1,
				Distribution: dist,
				Seed:         42,
				CodeSize:     16,
			}

			var buf bytes.Buffer
			gen := NewGenerator(cfg)

			sum, err := gen.Generate(&buf)
			if err != nil {
				t.Fatalf("generation failed: %v", err)
			}

			if sum.StorageSlots == 0 {
				t.Error("expected some storage slots")
			}
			if sum.ContractsCreated != 100 {
				t.Errorf("contracts = %d, want 100", sum.ContractsCreated)
			}
		})
	}
}
