// Package report formats benchmark results into comparison tables.
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/weiihann/statoor/harness"
)

// Generate writes a markdown comparison table for the given results.
func Generate(w io.Writer, results []harness.Result) error {
	if len(results) == 0 {
		return fmt.Errorf("no results to report")
	}

	rootMatch := checkStateRoots(results)
	fastestMs := findFastest(results)

	// Header.
	fmt.Fprintln(w, "## Benchmark Results")
	fmt.Fprintln(w)

	// State root check.
	if rootMatch {
		fmt.Fprintln(w, "State roots: **all match**")
	} else {
		fmt.Fprintln(w, "State roots: **MISMATCH**")

		for _, r := range results {
			fmt.Fprintf(w, "  - %s: %s\n", r.Client, r.StateRoot)
		}
	}

	fmt.Fprintln(w)

	// Table header.
	fmt.Fprintln(w, "| Client | Elapsed | Trie Time | DB Write "+
		"| Peak Mem | DB Size | Speedup |")
	fmt.Fprintln(w, "|--------|---------|-----------|----------"+
		"|----------|---------|---------|")

	for _, r := range results {
		speedup := 1.0
		if fastestMs > 0 && r.ElapsedMs > 0 {
			speedup = float64(r.ElapsedMs) / float64(fastestMs)
		}

		fmt.Fprintf(w, "| %s | %s | %s | %s | %s | %s | %.2fx |\n",
			r.Client,
			formatMs(r.ElapsedMs),
			formatMs(r.TrieTimeMs),
			formatMs(r.DBWriteTimeMs),
			formatBytes(r.PeakMemoryBytes),
			formatBytes(r.DBSizeBytes),
			speedup,
		)
	}

	fmt.Fprintln(w)

	// Detail rows.
	fmt.Fprintln(w, "| Client | Accounts | Contracts | Storage Slots |")
	fmt.Fprintln(w, "|--------|----------|-----------|---------------|")

	for _, r := range results {
		fmt.Fprintf(w, "| %s | %d | %d | %d |\n",
			r.Client,
			r.AccountsCreated,
			r.ContractsCreated,
			r.StorageSlots,
		)
	}

	return nil
}

// GenerateJSON writes results as JSON to w.
func GenerateJSON(w io.Writer, results []harness.Result) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	return enc.Encode(results)
}

func checkStateRoots(results []harness.Result) bool {
	if len(results) < 2 {
		return true
	}

	first := results[0].StateRoot
	for _, r := range results[1:] {
		if r.StateRoot != first {
			return false
		}
	}

	return true
}

func findFastest(results []harness.Result) int64 {
	fastest := int64(math.MaxInt64)
	for _, r := range results {
		if r.ElapsedMs > 0 && r.ElapsedMs < fastest {
			fastest = r.ElapsedMs
		}
	}

	if fastest == math.MaxInt64 {
		return 0
	}

	return fastest
}

func formatMs(ms int64) string {
	if ms < 1000 {
		return fmt.Sprintf("%dms", ms)
	}

	return fmt.Sprintf("%.2fs", float64(ms)/1000)
}

func formatBytes(b uint64) string {
	if b == 0 {
		return "-"
	}

	units := []string{"B", "KB", "MB", "GB", "TB"}
	size := float64(b)
	unit := 0

	for size >= 1024 && unit < len(units)-1 {
		size /= 1024
		unit++
	}

	formatted := fmt.Sprintf("%.1f", size)
	formatted = strings.TrimRight(formatted, "0")
	formatted = strings.TrimRight(formatted, ".")

	return formatted + " " + units[unit]
}
