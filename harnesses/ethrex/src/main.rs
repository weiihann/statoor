/// Ethrex harness reads a JSONL workload from stdin, applies state
/// operations using ethrex's native state/trie layer, and outputs
/// benchmark results as JSON to stdout.
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::process;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use clap::Parser;
use ethrex_common::types::{AccountInfo, AccountUpdate, Code};
use ethrex_common::{Address, H256, U256};
use ethrex_rlp::encode::RLPEncode;
use ethrex_storage::api::StorageBackend;
use ethrex_storage::api::tables::{ACCOUNT_CODES, ACCOUNT_TRIE_NODES, STORAGE_TRIE_NODES};
use ethrex_storage::backend::rocksdb::RocksDBBackend;
use ethrex_storage::{AccountUpdatesList, Store, apply_prefix};
use ethrex_trie::EMPTY_TRIE_HASH;
use serde::{Deserialize, Serialize};

#[derive(Parser)]
#[command(about = "Ethrex state benchmark harness")]
struct Cli {
    /// Database directory path
    #[arg(long)]
    db: String,
}

#[derive(Deserialize)]
struct Operation {
    op: String,
    #[serde(default)]
    address: String,
    #[serde(default)]
    balance: String,
    #[serde(default)]
    nonce: u64,
    #[serde(default)]
    code: String,
    #[serde(default)]
    slot: String,
    #[serde(default)]
    value: String,
}

#[derive(Serialize)]
struct BenchResult {
    client: String,
    state_root: String,
    accounts_created: usize,
    contracts_created: usize,
    storage_slots: usize,
    elapsed_ms: u128,
    trie_time_ms: u128,
    db_write_time_ms: u128,
    peak_memory_bytes: u64,
}

fn main() {
    let cli = Cli::parse();
    let start = Instant::now();

    // Use in-memory store for trie operations (avoids disk I/O
    // during the trie computation phase).
    let store = match Store::new(&cli.db, ethrex_storage::EngineType::InMemory) {
        Ok(s) => s,
        Err(e) => fatal(&format!("open store: {e}")),
    };

    let mut state_trie = match store.open_state_trie(*EMPTY_TRIE_HASH) {
        Ok(t) => t,
        Err(e) => fatal(&format!("open state trie: {e}")),
    };

    // Open RocksDB backend separately for the DB write phase.
    let db_backend: Arc<dyn StorageBackend> = match RocksDBBackend::open(&cli.db) {
        Ok(b) => Arc::new(b),
        Err(e) => fatal(&format!("open rocksdb: {e}")),
    };

    let mut accounts_created: usize = 0;
    let mut contracts_created: usize = 0;
    let mut storage_slots: usize = 0;

    // Accumulate updates per address so each address has one
    // AccountUpdate with all its fields merged.
    let mut updates: HashMap<Address, AccountUpdate> = HashMap::new();

    let stdin = io::stdin();
    for line_result in stdin.lock().lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => fatal(&format!("read stdin: {e}")),
        };
        if line.is_empty() {
            continue;
        }

        let op: Operation = match serde_json::from_str(&line) {
            Ok(o) => o,
            Err(e) => fatal(&format!("decode operation: {e}")),
        };

        match op.op.as_str() {
            "create_account" => {
                let addr = parse_address(&op.address);
                let balance = parse_u256(&op.balance);
                let code_hash = *ethrex_common::constants::EMPTY_KECCACK_HASH;

                let update = updates
                    .entry(addr)
                    .or_insert_with(|| AccountUpdate::new(addr));
                update.info = Some(AccountInfo {
                    code_hash,
                    balance,
                    nonce: op.nonce,
                });
                accounts_created += 1;
            }
            "set_code" => {
                let addr = parse_address(&op.address);
                let bytecode = hex_decode(&op.code);
                let code = Code::from_bytecode(Bytes::from(bytecode));

                let update = updates
                    .entry(addr)
                    .or_insert_with(|| AccountUpdate::new(addr));
                if let Some(info) = &mut update.info {
                    info.code_hash = code.hash;
                } else {
                    update.info = Some(AccountInfo {
                        code_hash: code.hash,
                        balance: U256::zero(),
                        nonce: 0,
                    });
                }
                update.code = Some(code);
                contracts_created += 1;
            }
            "set_storage" => {
                let addr = parse_address(&op.address);
                let slot = parse_h256(&op.slot);
                let value = parse_u256(&op.value);

                let update = updates
                    .entry(addr)
                    .or_insert_with(|| AccountUpdate::new(addr));
                update.added_storage.insert(slot, value);
                storage_slots += 1;
            }
            "compute_root" => {
                let update_list: Vec<AccountUpdate> = updates.into_values().collect();

                emit_result(
                    &store,
                    &mut state_trie,
                    &update_list,
                    &db_backend,
                    start,
                    accounts_created,
                    contracts_created,
                    storage_slots,
                );
                return;
            }
            other => fatal(&format!("unknown operation: {other}")),
        }
    }

    fatal("no compute_root operation found");
}

#[allow(clippy::too_many_arguments)]
fn emit_result(
    store: &Store,
    state_trie: &mut ethrex_trie::Trie,
    account_updates: &[AccountUpdate],
    db_backend: &Arc<dyn StorageBackend>,
    start: Instant,
    accounts_created: usize,
    contracts_created: usize,
    storage_slots: usize,
) {
    // Phase 1: Apply updates to the trie (trie time).
    let trie_start = Instant::now();
    let updates_list =
        match store.apply_account_updates_from_trie_batch(state_trie, account_updates) {
            Ok(u) => u,
            Err(e) => fatal(&format!("apply account updates: {e}")),
        };
    let trie_ms = trie_start.elapsed().as_millis();

    let state_root = updates_list.state_trie_hash;

    // Phase 2: Persist trie nodes to RocksDB (db write time).
    let db_start = Instant::now();
    write_updates_to_db(db_backend, &updates_list);
    let db_write_ms = db_start.elapsed().as_millis();

    let peak_memory = get_peak_memory_bytes();

    let result = BenchResult {
        client: "ethrex".to_string(),
        state_root: format!("{state_root:#x}"),
        accounts_created,
        contracts_created,
        storage_slots,
        elapsed_ms: start.elapsed().as_millis(),
        trie_time_ms: trie_ms,
        db_write_time_ms: db_write_ms,
        peak_memory_bytes: peak_memory,
    };

    match serde_json::to_writer(io::stdout(), &result) {
        Ok(()) => {
            println!();
        }
        Err(e) => fatal(&format!("encode result: {e}")),
    }
}

fn write_updates_to_db(backend: &Arc<dyn StorageBackend>, updates_list: &AccountUpdatesList) {
    let mut tx = match backend.begin_write() {
        Ok(tx) => tx,
        Err(e) => fatal(&format!("begin write: {e}")),
    };

    // Write state trie nodes
    for (nibbles, node_rlp) in &updates_list.state_updates {
        let key = nibbles.as_ref();
        if let Err(e) = tx.put(ACCOUNT_TRIE_NODES, key, node_rlp) {
            fatal(&format!("write state trie node: {e}"));
        }
    }

    // Write storage trie nodes (prefixed by account hash)
    for (account_hash, storage_nodes) in &updates_list.storage_updates {
        for (nibbles, node_rlp) in storage_nodes {
            let prefixed = apply_prefix(Some(*account_hash), nibbles.clone());
            let key = prefixed.into_vec();
            if let Err(e) = tx.put(STORAGE_TRIE_NODES, &key, node_rlp) {
                fatal(&format!("write storage trie node: {e}"));
            }
        }
    }

    // Write contract code
    for (code_hash, code) in &updates_list.code_updates {
        let key = code_hash.as_bytes();
        let value = code.bytecode.as_ref().encode_to_vec();
        if let Err(e) = tx.put(ACCOUNT_CODES, key, &value) {
            fatal(&format!("write account code: {e}"));
        }
    }

    if let Err(e) = tx.commit() {
        fatal(&format!("commit writes: {e}"));
    }
}

fn get_peak_memory_bytes() -> u64 {
    // Read VmPeak from /proc/self/status on Linux
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmPeak:") {
                let trimmed = rest.trim().trim_end_matches(" kB").trim();
                if let Ok(kb) = trimmed.parse::<u64>() {
                    return kb * 1024;
                }
            }
        }
    }
    0
}

fn parse_address(s: &str) -> Address {
    let bytes = hex_decode(s);
    if bytes.len() != 20 {
        fatal(&format!(
            "invalid address: expected 20 bytes, got {}",
            bytes.len()
        ));
    }
    Address::from_slice(&bytes)
}

fn parse_h256(s: &str) -> H256 {
    let bytes = hex_decode(s);
    if bytes.len() != 32 {
        fatal(&format!(
            "invalid H256: expected 32 bytes, got {}",
            bytes.len()
        ));
    }
    H256::from_slice(&bytes)
}

fn parse_u256(s: &str) -> U256 {
    if s.is_empty() {
        return U256::zero();
    }
    let bytes = hex_decode(s);
    U256::from_big_endian(&bytes)
}

fn hex_decode(s: &str) -> Vec<u8> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    // Pad odd-length hex strings with a leading zero
    if !s.len().is_multiple_of(2) {
        let padded = format!("0{s}");
        match hex::decode(&padded) {
            Ok(b) => return b,
            Err(e) => fatal(&format!("decode hex {s:?}: {e}")),
        }
    }
    match hex::decode(s) {
        Ok(b) => b,
        Err(e) => fatal(&format!("decode hex {s:?}: {e}")),
    }
}

fn fatal(msg: &str) -> ! {
    eprintln!("ethrex-harness: {msg}");
    process::exit(1);
}
