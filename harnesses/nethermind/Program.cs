// Nethermind harness reads a JSONL workload from stdin, applies state
// operations using Nethermind's native WorldState/PatriciaTree layer,
// and outputs benchmark results as JSON to stdout.

using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Db;
using Nethermind.Db.Rocks;
using Nethermind.Db.Rocks.Config;
using Nethermind.Evm.State;
using Nethermind.Evm.Tracing.State;
using Nethermind.Int256;
using Nethermind.Logging;
using Nethermind.Specs.Forks;
using Nethermind.State;
using Nethermind.Trie;
using Nethermind.Trie.Pruning;

string? dbDir = null;
for (int i = 0; i < args.Length; i++)
{
    if (args[i] == "--db" && i + 1 < args.Length)
    {
        dbDir = args[i + 1];
        i++;
    }
}

if (dbDir is null)
{
    Fatal("--db flag is required");
    return;
}

Stopwatch totalSw = Stopwatch.StartNew();

IKeyValueStoreWithBatching stateDb = CreateStateDb(dbDir);
MemDb codeDb = new();

PruningConfig pruningConfig = new();
SimpleFinalizedStateProvider finalizedProvider =
    new(pruningConfig.PruningBoundary);

TrieStore trieStore = new(
    new NodeStorage(stateDb),
    No.Pruning,
    Persist.EveryBlock,
    finalizedProvider,
    pruningConfig,
    LimboLogs.Instance);
finalizedProvider.TrieStore = trieStore;

TrieStoreScopeProvider scopeProvider = new(
    trieStore, codeDb, LimboLogs.Instance);
WorldState worldState = new(scopeProvider, LimboLogs.Instance);

using IDisposable scope = worldState.BeginScope(null);

var spec = Frontier.Instance;

int accounts = 0;
int contracts = 0;
int slots = 0;

using StreamReader reader = new(
    Console.OpenStandardInput(), bufferSize: 1 << 20);

while (reader.ReadLine() is { } line)
{
    if (string.IsNullOrWhiteSpace(line))
        continue;

    Operation op = JsonSerializer.Deserialize(
        line, OperationContext.Default.Operation)
        ?? throw new InvalidOperationException(
            "Failed to deserialize operation");

    switch (op.Op)
    {
        case "create_account":
        {
            Address addr = new(op.Address!);
            UInt256 balance = ParseBalance(op.Balance);
            UInt256 nonce = new((ulong)op.Nonce);
            worldState.CreateAccount(addr, balance, nonce);
            accounts++;
            break;
        }

        case "set_code":
        {
            Address addr = new(op.Address!);
            byte[] code = Convert.FromHexString(
                StripHexPrefix(op.Code!));
            ValueHash256 codeHash = KeccakCache.Compute(code);
            worldState.InsertCode(
                addr, codeHash, code, spec, isGenesis: true);
            contracts++;
            break;
        }

        case "set_storage":
        {
            Address addr = new(op.Address!);
            UInt256 slot = ParseUInt256(op.Slot!);
            byte[] value = PadLeft32(
                Convert.FromHexString(
                    StripHexPrefix(op.Value!)));
            StorageCell cell = new(addr, slot);
            worldState.Set(cell, value);
            slots++;
            break;
        }

        case "compute_root":
        {
            EmitResult(
                worldState, trieStore, totalSw,
                spec, accounts, contracts, slots);

            trieStore.Dispose();
            if (stateDb is IDisposable disposable)
                disposable.Dispose();
            return;
        }

        default:
            Fatal($"unknown operation: {op.Op}");
            return;
    }
}

Fatal("no compute_root operation found");

static void EmitResult(
    WorldState worldState,
    TrieStore trieStore,
    Stopwatch totalSw,
    Nethermind.Core.Specs.IReleaseSpec spec,
    int accounts,
    int contracts,
    int slots)
{
    Stopwatch trieSw = Stopwatch.StartNew();

    worldState.Commit(
        spec, NullStateTracer.Instance,
        isGenesis: true, commitRoots: true);
    worldState.CommitTree(0);

    long trieMs = trieSw.ElapsedMilliseconds;

    Stopwatch dbSw = Stopwatch.StartNew();
    trieStore.PersistCache(CancellationToken.None);
    long dbWriteMs = dbSw.ElapsedMilliseconds;

    Hash256 root = worldState.StateRoot;

    long peakMemory = Process.GetCurrentProcess().PeakWorkingSet64;

    Result result = new()
    {
        Client = "nethermind",
        StateRoot = root.ToString(),
        AccountsCreated = accounts,
        ContractsCreated = contracts,
        StorageSlots = slots,
        ElapsedMs = totalSw.ElapsedMilliseconds,
        TrieTimeMs = trieMs,
        DbWriteTimeMs = dbWriteMs,
        PeakMemoryBytes = peakMemory
    };

    string json = JsonSerializer.Serialize(
        result, ResultContext.Default.Result);
    Console.WriteLine(json);
}

static IKeyValueStoreWithBatching CreateStateDb(string dbDir)
{
    Directory.CreateDirectory(dbDir);
    DbSettings settings = new("State", "state");
    DbConfig dbConfig = new();
    PruningConfig pruning = new();
    HardwareInfo hwInfo = new();
    RocksDbConfigFactory configFactory = new(
        dbConfig, pruning, hwInfo, LimboLogs.Instance);
    return new DbOnTheRocks(
        dbDir, settings, dbConfig, configFactory,
        LimboLogs.Instance);
}

static UInt256 ParseBalance(string? hex)
{
    if (string.IsNullOrEmpty(hex))
        return UInt256.Zero;
    byte[] bytes = Convert.FromHexString(StripHexPrefix(hex));
    return new UInt256(bytes, isBigEndian: true);
}

static UInt256 ParseUInt256(string hex)
{
    byte[] bytes = Convert.FromHexString(StripHexPrefix(hex));
    return new UInt256(bytes, isBigEndian: true);
}

static string StripHexPrefix(string s)
{
    return s.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
        ? s[2..] : s;
}

static byte[] PadLeft32(byte[] input)
{
    if (input.Length >= 32)
        return input;
    byte[] padded = new byte[32];
    input.CopyTo(padded, 32 - input.Length);
    return padded;
}

static void Fatal(string message)
{
    Console.Error.WriteLine($"nethermind-harness: {message}");
    Environment.Exit(1);
}

// Minimal IFinalizedStateProvider for standalone use.
sealed class SimpleFinalizedStateProvider(long depth)
    : IFinalizedStateProvider
{
    public TrieStore TrieStore { get; set; } = null!;

    public long FinalizedBlockNumber =>
        TrieStore.LatestCommittedBlockNumber - depth;

    public Hash256? GetFinalizedStateRootAt(long blockNumber)
    {
        using var commitSets =
            TrieStore.CommitSetQueue
                .GetCommitSetsAtBlockNumber(blockNumber);
        if (commitSets.Count != 1) return null;
        return commitSets[0].StateRoot;
    }
}

sealed class Operation
{
    [JsonPropertyName("op")]
    public string Op { get; set; } = "";

    [JsonPropertyName("address")]
    public string? Address { get; set; }

    [JsonPropertyName("balance")]
    public string? Balance { get; set; }

    [JsonPropertyName("nonce")]
    public ulong Nonce { get; set; }

    [JsonPropertyName("code")]
    public string? Code { get; set; }

    [JsonPropertyName("slot")]
    public string? Slot { get; set; }

    [JsonPropertyName("value")]
    public string? Value { get; set; }
}

sealed class Result
{
    [JsonPropertyName("client")]
    public string Client { get; set; } = "";

    [JsonPropertyName("state_root")]
    public string StateRoot { get; set; } = "";

    [JsonPropertyName("accounts_created")]
    public int AccountsCreated { get; set; }

    [JsonPropertyName("contracts_created")]
    public int ContractsCreated { get; set; }

    [JsonPropertyName("storage_slots")]
    public int StorageSlots { get; set; }

    [JsonPropertyName("elapsed_ms")]
    public long ElapsedMs { get; set; }

    [JsonPropertyName("trie_time_ms")]
    public long TrieTimeMs { get; set; }

    [JsonPropertyName("db_write_time_ms")]
    public long DbWriteTimeMs { get; set; }

    [JsonPropertyName("peak_memory_bytes")]
    public long PeakMemoryBytes { get; set; }
}

[JsonSerializable(typeof(Operation))]
partial class OperationContext : JsonSerializerContext;

[JsonSerializable(typeof(Result))]
partial class ResultContext : JsonSerializerContext;
