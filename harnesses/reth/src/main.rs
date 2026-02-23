/// Reth harness reads a JSONL workload from stdin, applies state
/// operations using reth's native MDBX + trie layer, and outputs
/// benchmark results as JSON to stdout.
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::time::Instant;

use alloy_primitives::{Address, B256, U256, keccak256};
use clap::Parser;
use reth_db::mdbx::DatabaseArguments;
use reth_db::{DatabaseEnv, init_db, tables};
use reth_db_api::database::Database;
use reth_db_api::models::ClientVersion;
use reth_db_api::transaction::{DbTx, DbTxMut};
use reth_primitives_traits::{Account, Bytecode, StorageEntry};
use reth_trie::StateRoot;
use reth_trie_db::DatabaseStateRoot;
use serde::{Deserialize, Serialize};

#[derive(Parser)]
struct Cli {
    /// Path to the MDBX database directory.
    #[arg(long)]
    db: PathBuf,
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
    client: &'static str,
    state_root: String,
    accounts_created: u64,
    contracts_created: u64,
    storage_slots: u64,
    elapsed_ms: u64,
    trie_time_ms: u64,
    db_write_time_ms: u64,
    peak_memory_bytes: u64,
}

fn main() {
    let cli = Cli::parse();
    let start = Instant::now();

    let db = init_db(&cli.db, DatabaseArguments::new(ClientVersion::default()))
        .unwrap_or_else(|e| fatal(&format!("open mdbx: {e}")));

    let mut accounts: u64 = 0;
    let mut contracts: u64 = 0;
    let mut slots: u64 = 0;

    // Track per-address account state so set_code can update
    // the bytecode_hash after create_account.
    let mut account_map: HashMap<Address, Account> = HashMap::new();

    // Collect all writes, commit once before trie computation.
    let mut pending_accounts: Vec<(B256, Account)> = Vec::new();
    let mut pending_bytecodes: Vec<(B256, Bytecode)> = Vec::new();
    let mut pending_storage: Vec<(B256, StorageEntry)> = Vec::new();

    let stdin = io::stdin().lock();
    for line in stdin.lines() {
        let line = line.unwrap_or_else(|e| fatal(&format!("read stdin: {e}")));
        let op: Operation = serde_json::from_str(&line)
            .unwrap_or_else(|e| fatal(&format!("decode operation: {e}")));

        match op.op.as_str() {
            "create_account" => {
                let address = parse_address(&op.address);
                let balance = parse_u256(&op.balance);
                let account = Account {
                    nonce: op.nonce,
                    balance,
                    bytecode_hash: None,
                };
                let hashed = keccak256(address);
                pending_accounts.push((hashed, account));
                account_map.insert(address, account);
                accounts += 1;
            }
            "set_code" => {
                let address = parse_address(&op.address);
                let code_bytes = parse_hex(&op.code);
                let code_hash = keccak256(&code_bytes);
                let bytecode = Bytecode::new_raw(code_bytes.into());
                pending_bytecodes.push((code_hash, bytecode));

                let account = account_map.get(&address).copied().unwrap_or_default();
                let updated = Account {
                    bytecode_hash: Some(code_hash),
                    ..account
                };
                let hashed = keccak256(address);
                pending_accounts.push((hashed, updated));
                account_map.insert(address, updated);
                contracts += 1;
            }
            "set_storage" => {
                let address = parse_address(&op.address);
                let slot = parse_b256(&op.slot);
                let value = parse_u256(&op.value);
                let hashed_address = keccak256(address);
                let hashed_slot = keccak256(slot);
                pending_storage.push((
                    hashed_address,
                    StorageEntry {
                        key: hashed_slot,
                        value,
                    },
                ));
                slots += 1;
            }
            "compute_root" => {
                let db_write_ms =
                    flush_writes(&db, &pending_accounts, &pending_bytecodes, &pending_storage);
                emit_result(&db, start, accounts, contracts, slots, db_write_ms);
                return;
            }
            other => fatal(&format!("unknown operation: {other}")),
        }
    }

    fatal("no compute_root operation found");
}

/// Writes all pending state to MDBX in a single transaction.
/// Returns the time spent writing in milliseconds.
fn flush_writes(
    db: &DatabaseEnv,
    accounts: &[(B256, Account)],
    bytecodes: &[(B256, Bytecode)],
    storage: &[(B256, StorageEntry)],
) -> u64 {
    let db_start = Instant::now();

    let tx = db
        .tx_mut()
        .unwrap_or_else(|e| fatal(&format!("begin write tx: {e}")));

    for (hashed_address, account) in accounts {
        tx.put::<tables::HashedAccounts>(*hashed_address, *account)
            .unwrap_or_else(|e| fatal(&format!("put HashedAccounts: {e}")));
    }

    for (code_hash, bytecode) in bytecodes {
        tx.put::<tables::Bytecodes>(*code_hash, bytecode.clone())
            .unwrap_or_else(|e| fatal(&format!("put Bytecodes: {e}")));
    }

    for (hashed_address, entry) in storage {
        tx.put::<tables::HashedStorages>(*hashed_address, *entry)
            .unwrap_or_else(|e| fatal(&format!("put HashedStorages: {e}")));
    }

    tx.commit()
        .unwrap_or_else(|e| fatal(&format!("commit tx: {e}")));

    db_start.elapsed().as_millis() as u64
}

fn emit_result(
    db: &DatabaseEnv,
    start: Instant,
    accounts: u64,
    contracts: u64,
    slots: u64,
    db_write_ms: u64,
) {
    let trie_start = Instant::now();
    let tx = db
        .tx()
        .unwrap_or_else(|e| fatal(&format!("begin read tx: {e}")));
    let root = StateRoot::from_tx(&tx)
        .root()
        .unwrap_or_else(|e| fatal(&format!("compute state root: {e}")));
    let trie_ms = trie_start.elapsed().as_millis() as u64;

    let result = BenchResult {
        client: "reth",
        state_root: format!("{root:#x}"),
        accounts_created: accounts,
        contracts_created: contracts,
        storage_slots: slots,
        elapsed_ms: start.elapsed().as_millis() as u64,
        trie_time_ms: trie_ms,
        db_write_time_ms: db_write_ms,
        peak_memory_bytes: peak_memory_bytes(),
    };

    serde_json::to_writer(io::stdout(), &result)
        .unwrap_or_else(|e| fatal(&format!("encode result: {e}")));
    println!();
}

fn parse_address(s: &str) -> Address {
    s.parse()
        .unwrap_or_else(|e| fatal(&format!("parse address {s:?}: {e}")))
}

fn parse_b256(s: &str) -> B256 {
    s.parse()
        .unwrap_or_else(|e| fatal(&format!("parse B256 {s:?}: {e}")))
}

fn parse_u256(s: &str) -> U256 {
    if s.is_empty() {
        return U256::ZERO;
    }
    let stripped = s.strip_prefix("0x").unwrap_or(s);
    U256::from_be_slice(&hex_decode(stripped))
}

fn parse_hex(s: &str) -> Vec<u8> {
    let stripped = s.strip_prefix("0x").unwrap_or(s);
    hex_decode(stripped)
}

fn hex_decode(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(
                s.get(i..i + 2).unwrap_or_else(|| fatal("odd hex length")),
                16,
            )
            .unwrap_or_else(|e| fatal(&format!("decode hex: {e}")))
        })
        .collect()
}

fn peak_memory_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmHWM:").map(|v| {
                    let kb: u64 = v.trim().trim_end_matches(" kB").trim().parse().unwrap_or(0);
                    kb * 1024
                })
            })
        })
        .unwrap_or(0)
}

fn fatal(msg: &str) -> ! {
    eprintln!("reth-harness: {msg}");
    std::process::exit(1);
}
