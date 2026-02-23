package io.github.statoor.besu;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.storage.keyvalue.WorldStatePreimageKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.worldview.ForestMutableWorldState;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Besu harness for statoor benchmarking.
 *
 * <p>Reads JSONL workload from stdin, applies state operations
 * using Besu's Forest Merkle Patricia Trie, and outputs
 * benchmark results as JSON to stdout.
 */
public final class Main {

    private Main() {}

    public static void main(String[] args) {
        String dbDir = parseDbFlag(args);
        if (dbDir == null) {
            fatal("--db flag is required");
        }

        long startNanos = System.nanoTime();

        ForestWorldStateKeyValueStorage stateStorage =
            new ForestWorldStateKeyValueStorage(
                new InMemoryKeyValueStorage());
        WorldStatePreimageKeyValueStorage preimageStorage =
            new WorldStatePreimageKeyValueStorage(
                new InMemoryKeyValueStorage());

        MutableWorldState worldState = new ForestMutableWorldState(
            stateStorage, preimageStorage, EvmConfiguration.DEFAULT);

        WorldUpdater updater = worldState.updater();

        int accounts = 0;
        int contracts = 0;
        int slots = 0;

        ObjectMapper mapper = new ObjectMapper();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.strip();
                if (line.isEmpty()) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> op = mapper.readValue(
                    line, Map.class);
                String opType = (String) op.get("op");

                switch (opType) {
                    case "create_account" -> {
                        Address addr = Address.fromHexString(
                            (String) op.get("address"));
                        long nonce = parseNonce(op.get("nonce"));
                        Wei balance = parseBalance(
                            (String) op.get("balance"));
                        MutableAccount account =
                            updater.createAccount(addr, nonce, balance);
                        accounts++;
                    }
                    case "set_code" -> {
                        Address addr = Address.fromHexString(
                            (String) op.get("address"));
                        Bytes code = Bytes.fromHexString(
                            (String) op.get("code"));
                        MutableAccount account = updater.getAccount(addr);
                        if (account == null) {
                            fatal("set_code: account %s not found",
                                op.get("address"));
                        }
                        account.setCode(code);
                        contracts++;
                    }
                    case "set_storage" -> {
                        Address addr = Address.fromHexString(
                            (String) op.get("address"));
                        UInt256 slot = UInt256.fromHexString(
                            (String) op.get("slot"));
                        UInt256 value = UInt256.fromHexString(
                            (String) op.get("value"));
                        MutableAccount account = updater.getAccount(addr);
                        if (account == null) {
                            fatal("set_storage: account %s not found",
                                op.get("address"));
                        }
                        account.setStorageValue(slot, value);
                        slots++;
                    }
                    case "compute_root" -> {
                        emitResult(
                            worldState, updater,
                            startNanos, accounts, contracts, slots);
                        return;
                    }
                    default ->
                        fatal("unknown operation: %s", opType);
                }
            }
        } catch (Exception e) {
            fatal("read stdin: %s", e.getMessage());
        }

        fatal("no compute_root operation found");
    }

    private static void emitResult(
            MutableWorldState worldState,
            WorldUpdater updater,
            long startNanos,
            int accounts,
            int contracts,
            int slots) {

        long trieStart = System.nanoTime();
        updater.commit();
        long trieNanos = System.nanoTime() - trieStart;

        long dbStart = System.nanoTime();
        worldState.persist(null);
        long dbWriteNanos = System.nanoTime() - dbStart;

        String rootHash = worldState.rootHash().toHexString();

        Runtime runtime = Runtime.getRuntime();
        long peakMemory = runtime.totalMemory() - runtime.freeMemory();

        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        long trieMs = trieNanos / 1_000_000;
        long dbWriteMs = dbWriteNanos / 1_000_000;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("client", "besu");
        result.put("state_root", rootHash);
        result.put("accounts_created", accounts);
        result.put("contracts_created", contracts);
        result.put("storage_slots", slots);
        result.put("elapsed_ms", elapsedMs);
        result.put("trie_time_ms", trieMs);
        result.put("db_write_time_ms", dbWriteMs);
        result.put("peak_memory_bytes", peakMemory);

        try {
            ObjectMapper mapper = new ObjectMapper();
            System.out.println(mapper.writeValueAsString(result));
        } catch (Exception e) {
            fatal("encode result: %s", e.getMessage());
        }
    }

    private static String parseDbFlag(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--db".equals(args[i])) {
                return args[i + 1];
            }
        }
        return null;
    }

    private static long parseNonce(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number num) {
            return num.longValue();
        }
        return Long.parseLong(value.toString());
    }

    private static Wei parseBalance(String hex) {
        if (hex == null || hex.isEmpty()) {
            return Wei.ZERO;
        }
        return Wei.fromHexString(hex);
    }

    private static void fatal(String format, Object... args) {
        System.err.printf("besu-harness: " + format + "%n", args);
        System.exit(1);
    }
}
