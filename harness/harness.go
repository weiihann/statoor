package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// RunConfig holds parameters for a single harness execution.
type RunConfig struct {
	WorkloadPath string
	DBDir        string
	Timeout      time.Duration
}

// Runner launches and manages a single harness binary.
type Runner struct {
	Name       string
	BinaryPath string
	Logger     *slog.Logger
}

// NewRunner creates a Runner for the named client.
func NewRunner(name, binaryPath string, logger *slog.Logger) *Runner {
	return &Runner{
		Name:       name,
		BinaryPath: binaryPath,
		Logger:     logger.With(slog.String("client", name)),
	}
}

// Run executes the harness binary and returns parsed results.
func (r *Runner) Run(ctx context.Context, cfg RunConfig) (*Result, error) {
	if cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}

	dbDir := filepath.Join(cfg.DBDir, r.Name)
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		return nil, fmt.Errorf("create db dir %s: %w", dbDir, err)
	}

	cmd := exec.CommandContext(ctx, r.BinaryPath, "--db", dbDir)

	workloadFile, err := os.Open(cfg.WorkloadPath)
	if err != nil {
		return nil, fmt.Errorf("open workload %s: %w", cfg.WorkloadPath, err)
	}
	defer workloadFile.Close()

	cmd.Stdin = workloadFile

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	r.Logger.Info("starting harness",
		slog.String("binary", r.BinaryPath),
		slog.String("db_dir", dbDir),
	)

	wallStart := time.Now()

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf(
			"harness %s failed: %w\nstderr: %s",
			r.Name, err, stderr.String(),
		)
	}

	wallElapsed := time.Since(wallStart)

	r.Logger.Info("harness finished",
		slog.Duration("wall_time", wallElapsed),
	)

	result, err := parseResult(r.Name, &stdout)
	if err != nil {
		return nil, fmt.Errorf(
			"parse %s output: %w\nstdout: %s",
			r.Name, err, stdout.String(),
		)
	}

	dbSize, err := dirSize(dbDir)
	if err != nil {
		r.Logger.Warn("failed to measure db size",
			slog.String("error", err.Error()),
		)
	}

	result.DBSizeBytes = dbSize

	return result, nil
}

func parseResult(client string, r io.Reader) (*Result, error) {
	var result Result
	if err := json.NewDecoder(r).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode JSON: %w", err)
	}

	if result.Client == "" {
		result.Client = client
	}

	return &result, nil
}

func dirSize(path string) (uint64, error) {
	var size uint64

	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += uint64(info.Size())
		}

		return nil
	})

	return size, err
}
