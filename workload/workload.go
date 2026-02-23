// Package workload generates deterministic JSONL workloads for Ethereum
// state benchmarking. Each workload consists of create_account, set_code,
// set_storage, and compute_root operations.
package workload

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	mrand "math/rand"
)

// Operation represents a single state operation in the workload.
type Operation struct {
	Op      string `json:"op"`
	Address string `json:"address,omitempty"`
	Balance string `json:"balance,omitempty"`
	Nonce   uint64 `json:"nonce,omitempty"`
	Code    string `json:"code,omitempty"`
	Slot    string `json:"slot,omitempty"`
	Value   string `json:"value,omitempty"`
}

// Summary contains statistics about the generated workload.
type Summary struct {
	TotalOperations  int
	AccountsCreated  int
	ContractsCreated int
	StorageSlots     int
}

// Config controls workload generation parameters.
type Config struct {
	NumAccounts  int
	NumContracts int
	MaxSlots     int
	MinSlots     int
	Distribution string
	Seed         int64
	CodeSize     int
}

// Generator produces deterministic workloads from a Config.
type Generator struct {
	cfg Config
	rng *mrand.Rand
}

// NewGenerator creates a Generator from the given Config.
func NewGenerator(cfg Config) *Generator {
	return &Generator{
		cfg: cfg,
		rng: mrand.New(mrand.NewSource(cfg.Seed)),
	}
}

// Generate writes a JSONL workload to w and returns a Summary.
func (g *Generator) Generate(w io.Writer) (Summary, error) {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	var summary Summary

	// Generate EOAs.
	for i := 0; i < g.cfg.NumAccounts; i++ {
		addr := g.randomAddress()
		balance := g.randomBalance(1, 100)
		nonce := uint64(g.rng.Intn(100))

		if err := enc.Encode(Operation{
			Op:      "create_account",
			Address: addr,
			Balance: balance,
			Nonce:   nonce,
		}); err != nil {
			return summary, fmt.Errorf("encode create_account: %w", err)
		}

		summary.AccountsCreated++
		summary.TotalOperations++
	}

	// Generate contracts with code and storage.
	slotDist := g.slotDistribution()

	for i := 0; i < g.cfg.NumContracts; i++ {
		addr := g.randomAddress()
		balance := g.randomBalance(0, 100)
		nonce := uint64(g.rng.Intn(100))
		code := g.randomCode()

		if err := enc.Encode(Operation{
			Op:      "create_account",
			Address: addr,
			Balance: balance,
			Nonce:   nonce,
		}); err != nil {
			return summary, fmt.Errorf("encode create_account: %w", err)
		}

		summary.TotalOperations++

		if err := enc.Encode(Operation{
			Op:      "set_code",
			Address: addr,
			Code:    code,
		}); err != nil {
			return summary, fmt.Errorf("encode set_code: %w", err)
		}

		summary.TotalOperations++

		numSlots := slotDist[i]
		for j := 0; j < numSlots; j++ {
			slot := g.randomHash()
			value := g.randomNonZeroHash()

			if err := enc.Encode(Operation{
				Op:      "set_storage",
				Address: addr,
				Slot:    slot,
				Value:   value,
			}); err != nil {
				return summary, fmt.Errorf("encode set_storage: %w", err)
			}

			summary.TotalOperations++
			summary.StorageSlots++
		}

		summary.ContractsCreated++
	}

	// Final compute_root operation.
	if err := enc.Encode(Operation{Op: "compute_root"}); err != nil {
		return summary, fmt.Errorf("encode compute_root: %w", err)
	}

	summary.TotalOperations++

	return summary, nil
}

func (g *Generator) randomAddress() string {
	var buf [20]byte
	g.rng.Read(buf[:])

	return "0x" + hex.EncodeToString(buf[:])
}

func (g *Generator) randomHash() string {
	var buf [32]byte
	g.rng.Read(buf[:])

	return "0x" + hex.EncodeToString(buf[:])
}

func (g *Generator) randomNonZeroHash() string {
	var buf [32]byte
	g.rng.Read(buf[:])

	allZero := true
	for _, b := range buf {
		if b != 0 {
			allZero = false

			break
		}
	}

	if allZero {
		buf[31] = 1
	}

	return "0x" + hex.EncodeToString(buf[:])
}

func (g *Generator) randomBalance(minETH, maxETH int) string {
	eth := minETH + g.rng.Intn(maxETH-minETH+1)
	// Return balance in wei as hex string (eth * 1e18).
	// Use big-endian 32-byte representation.
	var buf [32]byte

	weiPerETH := uint64(1e18)
	val := uint64(eth) * weiPerETH

	// Encode as big-endian into the last 8 bytes.
	for i := 31; i >= 24; i-- {
		buf[i] = byte(val)
		val >>= 8
	}

	return "0x" + hex.EncodeToString(buf[:])
}

func (g *Generator) randomCode() string {
	size := g.cfg.CodeSize + g.rng.Intn(g.cfg.CodeSize)
	buf := make([]byte, size)
	g.rng.Read(buf)

	return "0x" + hex.EncodeToString(buf)
}

func (g *Generator) slotDistribution() []int {
	dist := make([]int, g.cfg.NumContracts)

	switch g.cfg.Distribution {
	case "power-law":
		alpha := 1.5
		for i := range dist {
			u := g.rng.Float64()
			slots := float64(g.cfg.MinSlots) / math.Pow(1-u, 1/alpha)
			if slots > float64(g.cfg.MaxSlots) {
				slots = float64(g.cfg.MaxSlots)
			}
			dist[i] = max(g.cfg.MinSlots, int(slots))
		}

	case "exponential":
		lambda := math.Log(2) / float64(g.cfg.MaxSlots/4)
		for i := range dist {
			u := g.rng.Float64()
			slots := -math.Log(1-u) / lambda
			clamped := math.Max(
				float64(g.cfg.MinSlots),
				math.Min(slots, float64(g.cfg.MaxSlots)),
			)
			dist[i] = int(clamped)
		}

	case "uniform":
		rng := g.cfg.MaxSlots - g.cfg.MinSlots + 1
		for i := range dist {
			dist[i] = g.cfg.MinSlots + g.rng.Intn(rng)
		}

	default:
		// Fall back to uniform if unknown distribution.
		rng := g.cfg.MaxSlots - g.cfg.MinSlots + 1
		for i := range dist {
			dist[i] = g.cfg.MinSlots + g.rng.Intn(rng)
		}
	}

	return dist
}
