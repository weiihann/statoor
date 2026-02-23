package harness

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
)

// BuildConfig describes how to build a harness binary for a client.
type BuildConfig struct {
	Name       string
	SourceDir  string
	BinaryPath string
}

// KnownClients returns the list of supported client names.
func KnownClients() []string {
	return []string{
		"geth", "erigon", "reth", "ethrex", "besu", "nethermind",
	}
}

// ResolveBinary returns the expected binary path for a client
// given the harnesses root directory.
func ResolveBinary(harnessesDir, client string) string {
	switch client {
	case "geth":
		return filepath.Join(harnessesDir, "geth", "geth-harness")
	case "erigon":
		return filepath.Join(harnessesDir, "erigon", "erigon-harness")
	case "reth":
		return filepath.Join(
			harnessesDir, "reth", "target", "release", "reth-harness",
		)
	case "ethrex":
		return filepath.Join(
			harnessesDir, "ethrex", "target", "release", "ethrex-harness",
		)
	case "besu":
		return filepath.Join(
			harnessesDir, "besu", "build", "libs", "besu-harness.jar",
		)
	case "nethermind":
		return filepath.Join(
			harnessesDir, "nethermind", "bin", "Release",
			"net10.0", "Nethermind.Harness",
		)
	default:
		return filepath.Join(harnessesDir, client, client+"-harness")
	}
}

// Build compiles a harness binary for the given client.
func Build(
	ctx context.Context,
	logger *slog.Logger,
	harnessesDir string,
	client string,
) (string, error) {
	srcDir := filepath.Join(harnessesDir, client)
	binPath := ResolveBinary(harnessesDir, client)

	logger.InfoContext(ctx, "building harness",
		slog.String("client", client),
		slog.String("source_dir", srcDir),
	)

	var cmd *exec.Cmd

	switch client {
	case "geth", "erigon":
		cmd = exec.CommandContext(
			ctx, "go", "build", "-o", binPath, ".",
		)
		cmd.Dir = srcDir

	case "reth", "ethrex":
		cmd = exec.CommandContext(
			ctx, "cargo", "build", "--release",
		)
		cmd.Dir = srcDir

	case "besu":
		gradlew := filepath.Join(srcDir, "gradlew")
		cmd = exec.CommandContext(ctx, gradlew, "shadowJar")
		cmd.Dir = srcDir

	case "nethermind":
		cmd = exec.CommandContext(
			ctx, "dotnet", "build", "-c", "Release",
		)
		cmd.Dir = srcDir

	default:
		return "", fmt.Errorf("unknown client %q", client)
	}

	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("build %s: %w", client, err)
	}

	if _, err := os.Stat(binPath); err != nil {
		return "", fmt.Errorf(
			"build %s: binary not found at %s", client, binPath,
		)
	}

	logger.InfoContext(ctx, "harness built",
		slog.String("client", client),
		slog.String("binary", binPath),
	)

	return binPath, nil
}

// CommandConfig holds the resolved command, extra arguments, and
// environment variables needed to run a harness binary.
type CommandConfig struct {
	Binary    string
	ExtraArgs []string
	Env       []string
}

// WrapCommand returns the exec configuration needed to run a harness
// binary. For most clients this is just the binary path, but besu
// needs java -jar and nethermind needs DOTNET_ROOT.
func WrapCommand(client, binPath string) CommandConfig {
	switch client {
	case "besu":
		return CommandConfig{
			Binary:    "java",
			ExtraArgs: []string{"-jar", binPath},
		}
	default:
		return CommandConfig{Binary: binPath}
	}
}
