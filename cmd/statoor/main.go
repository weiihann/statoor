// Package main provides the CLI entry point for statoor, a cross-client
// Ethereum state benchmarking tool.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/weiihann/statoor/harness"
	"github.com/weiihann/statoor/report"
	"github.com/weiihann/statoor/workload"
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
		harnessesDir string
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
				harnessesDir: harnessesDir,
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
	flags.StringVar(&harnessesDir, "harnesses-dir", "",
		"Path to harnesses directory (default: ./harnesses)")
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
	harnessesDir string
	skipBuild    bool
	outputJSON   bool
}

func runBenchmark(
	ctx context.Context,
	logger *slog.Logger,
	cfg runConfig,
) error {
	if len(cfg.clients) == 0 {
		return fmt.Errorf(
			"at least one client must be specified via --clients",
		)
	}

	logger.InfoContext(ctx, "starting benchmark",
		slog.Int("accounts", cfg.accounts),
		slog.Int("contracts", cfg.contracts),
		slog.Int("max_slots", cfg.maxSlots),
		slog.Int("min_slots", cfg.minSlots),
		slog.String("distribution", cfg.distribution),
		slog.Int64("seed", cfg.seed),
		slog.Any("clients", cfg.clients),
	)

	harnessesDir := cfg.harnessesDir
	if harnessesDir == "" {
		harnessesDir = "harnesses"
	}

	var err error

	harnessesDir, err = filepath.Abs(harnessesDir)
	if err != nil {
		return fmt.Errorf("resolve harnesses dir: %w", err)
	}

	// Step 1: Generate workload (or use pre-generated file).
	workloadPath := cfg.workloadPath
	if workloadPath == "" {
		workloadPath, err = generateWorkload(ctx, logger, cfg)
		if err != nil {
			return fmt.Errorf("generate workload: %w", err)
		}

		defer os.Remove(workloadPath)
	}

	// Step 2: Build harness binaries (unless --skip-build).
	binaries := make(map[string]string, len(cfg.clients))

	for _, client := range cfg.clients {
		binPath := harness.ResolveBinary(harnessesDir, client)

		if !cfg.skipBuild {
			binPath, err = harness.Build(ctx, logger, harnessesDir, client)
			if err != nil {
				return fmt.Errorf("build %s: %w", client, err)
			}
		}

		binaries[client] = binPath
	}

	// Step 3: Prepare DB directory.
	dbDir := cfg.dbDir
	if dbDir == "" {
		dbDir = "tmp"
	}

	if err = os.MkdirAll(dbDir, 0o755); err != nil {
		return fmt.Errorf("create db dir: %w", err)
	}

	// Step 4: Run each harness sequentially.
	results := make([]harness.Result, 0, len(cfg.clients))

	for _, client := range cfg.clients {
		binPath := binaries[client]
		cmdCfg := harness.WrapCommand(client, binPath)

		runner := harness.NewRunner(
			client, cmdCfg.Binary, cmdCfg.ExtraArgs, cmdCfg.Env, logger,
		)
		result, runErr := runner.Run(ctx, harness.RunConfig{
			WorkloadPath: workloadPath,
			DBDir:        dbDir,
			Timeout:      30 * time.Minute,
		})

		if runErr != nil {
			return fmt.Errorf("run %s: %w", client, runErr)
		}

		results = append(results, *result)
	}

	// Step 5: Generate report.
	if cfg.outputJSON {
		if err := report.GenerateJSON(os.Stdout, results); err != nil {
			return fmt.Errorf("generate JSON report: %w", err)
		}
	} else {
		if err := report.Generate(os.Stdout, results); err != nil {
			return fmt.Errorf("generate report: %w", err)
		}
	}

	logger.InfoContext(ctx, "benchmark complete")

	return nil
}

func generateWorkload(
	ctx context.Context,
	logger *slog.Logger,
	cfg runConfig,
) (string, error) {
	seed := cfg.seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	gen := workload.NewGenerator(workload.Config{
		NumAccounts:  cfg.accounts,
		NumContracts: cfg.contracts,
		MaxSlots:     cfg.maxSlots,
		MinSlots:     cfg.minSlots,
		Distribution: cfg.distribution,
		Seed:         seed,
		CodeSize:     cfg.codeSize,
	})

	tmpFile, err := os.CreateTemp("", "statoor-workload-*.jsonl")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}

	summary, err := gen.Generate(tmpFile)
	if err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())

		return "", fmt.Errorf("generate: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return "", fmt.Errorf("close workload file: %w", err)
	}

	logger.InfoContext(ctx, "workload generated",
		slog.String("path", tmpFile.Name()),
		slog.Int("operations", summary.TotalOperations),
		slog.Int("accounts", summary.AccountsCreated),
		slog.Int("contracts", summary.ContractsCreated),
		slog.Int("storage_slots", summary.StorageSlots),
	)

	return tmpFile.Name(), nil
}
