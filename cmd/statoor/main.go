// Package main provides the CLI entry point for statoor, a cross-client
// Ethereum state benchmarking tool.
package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	root := newRootCmd(logger)
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCmd(logger *slog.Logger) *cobra.Command {
	root := &cobra.Command{
		Use:   "statoor",
		Short: "Cross-client Ethereum state benchmarking tool",
		Long: `Statoor benchmarks all major Ethereum execution clients by running
the same deterministic workload through each client's native state/trie/database
layer and comparing performance metrics.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.AddCommand(newRunCmd(logger))

	return root
}

func newRunCmd(logger *slog.Logger) *cobra.Command {
	var (
		accounts     int
		contracts    int
		maxSlots     int
		minSlots     int
		distribution string
		seed         int64
		codeSize     int
		clients      []string
		dbDir        string
		workloadPath string
		skipBuild    bool
		outputJSON   bool
	)

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run state benchmarks across Ethereum clients",
		Long: `Generate a deterministic workload and run it through one or more
Ethereum client harnesses, comparing state roots and performance.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runBenchmark(cmd.Context(), logger, runConfig{
				accounts:     accounts,
				contracts:    contracts,
				maxSlots:     maxSlots,
				minSlots:     minSlots,
				distribution: distribution,
				seed:         seed,
				codeSize:     codeSize,
				clients:      clients,
				dbDir:        dbDir,
				workloadPath: workloadPath,
				skipBuild:    skipBuild,
				outputJSON:   outputJSON,
			})
		},
	}

	flags := cmd.Flags()
	flags.IntVar(&accounts, "accounts", 1000,
		"Number of EOA accounts to create")
	flags.IntVar(&contracts, "contracts", 100,
		"Number of contracts to create")
	flags.IntVar(&maxSlots, "max-slots", 10000,
		"Maximum storage slots per contract")
	flags.IntVar(&minSlots, "min-slots", 1,
		"Minimum storage slots per contract")
	flags.StringVar(&distribution, "distribution", "power-law",
		"Storage slot distribution: power-law, uniform, exponential")
	flags.Int64Var(&seed, "seed", 0,
		"Random seed (0 = use current time)")
	flags.IntVar(&codeSize, "code-size", 1024,
		"Average contract code size in bytes")
	flags.StringSliceVar(&clients, "clients", nil,
		"Clients to benchmark (e.g. geth,reth,erigon)")
	flags.StringVar(&dbDir, "db-dir", "",
		"Base directory for client databases")
	flags.StringVar(&workloadPath, "workload", "",
		"Path to pre-generated workload file (skip generation)")
	flags.BoolVar(&skipBuild, "skip-build", false,
		"Skip building harness binaries")
	flags.BoolVar(&outputJSON, "json", false,
		"Output results as JSON instead of table")

	return cmd
}

type runConfig struct {
	accounts     int
	contracts    int
	maxSlots     int
	minSlots     int
	distribution string
	seed         int64
	codeSize     int
	clients      []string
	dbDir        string
	workloadPath string
	skipBuild    bool
	outputJSON   bool
}

func runBenchmark(
	_ interface{ Done() <-chan struct{} },
	logger *slog.Logger,
	cfg runConfig,
) error {
	if len(cfg.clients) == 0 {
		return fmt.Errorf("at least one client must be specified via --clients")
	}

	logger.Info("starting benchmark",
		slog.Int("accounts", cfg.accounts),
		slog.Int("contracts", cfg.contracts),
		slog.Int("max_slots", cfg.maxSlots),
		slog.Int("min_slots", cfg.minSlots),
		slog.String("distribution", cfg.distribution),
		slog.Int64("seed", cfg.seed),
		slog.Any("clients", cfg.clients),
	)

	// TODO: Wire workload generation, harness execution, and reporting
	logger.Info("benchmark complete")

	return nil
}
