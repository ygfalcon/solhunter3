use std::{
    collections::HashMap,
    fs::OpenOptions,
    path::Path,
    sync::{Arc, OnceLock},
};

use tokio::sync::Mutex;

use std::time::{Duration, Instant};

static PAPER_KEYPAIR_WARNED: OnceLock<()> = OnceLock::new();
static EVENT_SCHEMA_VERSION: OnceLock<String> = OnceLock::new();

use anyhow::{anyhow, Result};
use futures_util::{stream::FuturesUnordered, SinkExt, StreamExt};
use memmap2::{MmapMut, MmapOptions};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use toml::value::Table as TomlTable;
pub mod solhunter_zero;
use solana_client::nonblocking::rpc_client::RpcClient;
use solhunter_zero as pb;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UnixListener},
    sync::{broadcast, mpsc},
};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message as WsMessage};
use sysinfo::System;
use zstd::stream::{decode_all, encode_all};

fn cfg_str(cfg: &TomlTable, key: &str) -> Option<String> {
    cfg.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

fn event_schema_version() -> &'static str {
    EVENT_SCHEMA_VERSION
        .get_or_init(|| std::env::var("EVENT_SCHEMA_VERSION").unwrap_or_else(|_| "v3".to_string()))
        .as_str()
}

fn event_bus_url(cfg: &TomlTable) -> Option<String> {
    std::env::var("EVENT_BUS_URL")
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| cfg_str(cfg, "event_bus_url"))
        .filter(|v| !v.is_empty())
}

fn depth_ws_addr(cfg: &TomlTable) -> (String, u16) {
    let addr = std::env::var("DEPTH_WS_ADDR")
        .ok()
        .or_else(|| cfg_str(cfg, "DEPTH_WS_ADDR"))
        .or_else(|| cfg_str(cfg, "depth_ws_addr"))
        .unwrap_or_else(|| "0.0.0.0".into());
    let port = std::env::var("DEPTH_WS_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .or_else(|| cfg_str(cfg, "DEPTH_WS_PORT").and_then(|v| v.parse().ok()))
        .or_else(|| cfg_str(cfg, "depth_ws_port").and_then(|v| v.parse().ok()))
        .unwrap_or(8766);
    (addr, port)
}

fn update_threshold(cfg: &TomlTable) -> f64 {
    std::env::var("DEPTH_UPDATE_THRESHOLD")
        .ok()
        .and_then(|v| v.parse().ok())
        .or_else(|| cfg_str(cfg, "depth_update_threshold").and_then(|v| v.parse().ok()))
        .unwrap_or(0.0)
}

fn send_interval(cfg: &TomlTable) -> Duration {
    let ms = std::env::var("DEPTH_MIN_SEND_INTERVAL")
        .ok()
        .and_then(|v| v.parse().ok())
        .or_else(|| cfg_str(cfg, "depth_min_send_interval").and_then(|v| v.parse().ok()))
        .unwrap_or(100u64);
    Duration::from_millis(ms)
}

fn heartbeat_interval(cfg: &TomlTable) -> Duration {
    let secs = std::env::var("DEPTH_HEARTBEAT_INTERVAL")
        .ok()
        .and_then(|v| v.parse().ok())
        .or_else(|| cfg_str(cfg, "depth_heartbeat_interval").and_then(|v| v.parse().ok()))
        .unwrap_or(30u64);
    Duration::from_secs(secs)
}

fn diff_updates(cfg: &TomlTable) -> bool {
    std::env::var("DEPTH_DIFF_UPDATES")
        .ok()
        .or_else(|| cfg_str(cfg, "depth_diff_updates"))
        .map(|v| !v.is_empty() && v != "0")
        .unwrap_or(false)
}

fn load_config(path: &str) -> Result<TomlTable> {
    let data = std::fs::read_to_string(path)?;
    let val: toml::Value = toml::from_str(&data)?;
    Ok(val.as_table().cloned().unwrap_or_default())
}

mod route;
use base64::{Engine, engine::general_purpose::STANDARD};
use bincode::{deserialize, serialize, Options};
use chrono::Utc;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::{
    commitment_config::CommitmentLevel,
    message::VersionedMessage,
    signature::{Keypair, Signer, read_keypair_file},
    system_instruction,
    transaction::{Transaction, VersionedTransaction},
};

#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq)]
struct TokenInfo {
    bids: f64,
    asks: f64,
    tx_rate: f64,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
struct TokenAgg {
    dex: HashMap<String, TokenInfo>,
    bids: f64,
    asks: f64,
    tx_rate: f64,
    ts: i64,
}

type DexMap = Arc<Mutex<HashMap<String, HashMap<String, TokenInfo>>>>;
type MempoolMap = Arc<Mutex<HashMap<String, f64>>>;

type SharedMmap = Arc<Mutex<MmapMut>>;
/// Mapping from token -> (offset, length) within the mmap data section
type OffsetMap = Arc<Mutex<HashMap<String, (usize, usize)>>>;
/// Next write offset within the mmap data section
type NextOffset = Arc<Mutex<usize>>;

type WsSender = broadcast::Sender<String>;
type WsBinSender = broadcast::Sender<Vec<u8>>;
type LatestSnap = Arc<Mutex<String>>;
type LatestBin = Arc<Mutex<Vec<u8>>>;
type LastAggMap = Arc<Mutex<HashMap<String, TokenAgg>>>;
/// Aggregated state of all tokens
type AggMap = Arc<Mutex<HashMap<String, TokenAgg>>>;
type AdjMap = Arc<Mutex<HashMap<String, route::AdjacencyMatrix>>>;
type AdjacencyCache = Arc<Mutex<HashMap<String, (HashMap<String, HashMap<String, TokenInfo>>, route::AdjacencyMatrix)>>>;
type LastSent = Arc<Mutex<Instant>>;

fn maybe_decompress(data: &[u8]) -> Vec<u8> {
    if data.len() > 4 && data.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        decode_all(data).unwrap_or_else(|_| data.to_vec())
    } else {
        data.to_vec()
    }
}

fn compress_event(data: &[u8]) -> Vec<u8> {
    let compress = std::env::var("COMPRESS_EVENTS")
        .map(|v| !v.is_empty() && v != "0")
        .unwrap_or(false);
    if !compress {
        return data.to_vec();
    }
    let algo = std::env::var("EVENT_COMPRESSION")
        .unwrap_or_else(|_| "zstd".to_string())
        .to_lowercase();
    if algo == "zstd" {
        encode_all(data, 0).unwrap_or_else(|_| data.to_vec())
    } else {
        data.to_vec()
    }
}

#[derive(Serialize, Deserialize)]
struct EventMessage<T> {
    topic: String,
    payload: T,
}

#[derive(Default, Clone)]
struct AutoExecEntry {
    threshold: f64,
    txs: Vec<String>,
}

type AutoExecMap = Arc<Mutex<HashMap<String, AutoExecEntry>>>;

struct ExecContext {
    client: RpcClient,
    keypair: Keypair,
    blockhash: Arc<Mutex<(solana_sdk::hash::Hash, Instant)>>,
}

impl ExecContext {
    async fn new(url: &str, keypair: Keypair) -> Self {
        let client = RpcClient::new(url.to_string());
        let bh = client.get_latest_blockhash().await.unwrap();
        Self {
            client,
            keypair,
            blockhash: Arc::new(Mutex::new((bh, Instant::now()))),
        }
    }

    async fn latest_blockhash(&self) -> solana_sdk::hash::Hash {
        let mut guard = self.blockhash.lock().await;
        if guard.1.elapsed() > Duration::from_secs(30) {
            if let Ok(new) = self.client.get_latest_blockhash().await {
                *guard = (new, Instant::now());
            }
        }
        guard.0
    }

    async fn send_dummy_tx(&self) -> Result<String> {
        let bh = self.latest_blockhash().await;
        let ix = system_instruction::transfer(&self.keypair.pubkey(), &self.keypair.pubkey(), 0);
        let tx = Transaction::new_signed_with_payer(
            &[ix],
            Some(&self.keypair.pubkey()),
            &[&self.keypair],
            bh,
        );
        let sig = self.client.send_transaction(&tx).await?;
        Ok(sig.to_string())
    }

    async fn send_raw_tx(&self, tx_b64: &str) -> Result<String> {
        let data = STANDARD.decode(tx_b64)?;
        let tx: VersionedTransaction = deserialize(&data)?;
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            ..RpcSendTransactionConfig::default()
        };
        let sig = self
            .client
            .send_transaction_with_config(&tx, config)
            .await?;
        Ok(sig.to_string())
    }

    async fn send_raw_tx_priority(
        &self,
        tx_b64: &str,
        priority: Option<Vec<String>>,
    ) -> Result<String> {
        let data = STANDARD.decode(tx_b64)?;
        let tx: VersionedTransaction = deserialize(&data)?;
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            ..RpcSendTransactionConfig::default()
        };
        if let Some(urls) = priority {
            let mut futs = FuturesUnordered::new();
            for url in urls {
                let tx = tx.clone();
                let conf = config.clone();
                futs.push(async move {
                    let client = RpcClient::new(url);
                    client
                        .send_transaction_with_config(&tx, conf)
                        .await
                        .map(|sig| sig.to_string())
                });
            }
            while let Some(res) = futs.next().await {
                if let Ok(sig) = res {
                    return Ok(sig);
                }
            }
        }
        let sig = self
            .client
            .send_transaction_with_config(&tx, config)
            .await?;
        Ok(sig.to_string())
    }

    async fn submit_signed_tx(&self, tx_b64: &str) -> Result<String> {
        self.send_raw_tx(tx_b64).await
    }

    async fn sign_template(&self, msg_b64: &str, priority_fee: Option<u64>) -> Result<String> {
        use solana_sdk::{compute_budget::ComputeBudgetInstruction, message::MessageHeader};

        let mut msg: VersionedMessage = deserialize(&STANDARD.decode(msg_b64)?)?;

        if let Some(fee) = priority_fee {
            fn insert_budget(
                header: &mut MessageHeader,
                keys: &mut Vec<solana_sdk::pubkey::Pubkey>,
                ixs: &mut Vec<solana_sdk::instruction::CompiledInstruction>,
                fee: u64,
            ) {
                use solana_program::message::AccountKeys;
                use solana_sdk::{compute_budget, message::v0::LoadedAddresses};
                let pid = compute_budget::id();
                if keys.iter().position(|k| *k == pid).is_none() {
                    keys.push(pid);
                    header.num_readonly_unsigned_accounts =
                        header.num_readonly_unsigned_accounts.saturating_add(1);
                };
                let accounts = AccountKeys::new(keys, None::<&LoadedAddresses>);
                let mut add_ix = |ix: solana_sdk::instruction::Instruction| {
                    let mut c = accounts.compile_instructions(&[ix]);
                    if let Some(ci) = c.pop() {
                        ixs.insert(0, ci);
                    }
                };
                add_ix(ComputeBudgetInstruction::set_compute_unit_price(fee));
                add_ix(ComputeBudgetInstruction::set_compute_unit_limit(1_400_000));
            }

            match &mut msg {
                VersionedMessage::Legacy(m) => {
                    insert_budget(&mut m.header, &mut m.account_keys, &mut m.instructions, fee);
                }
                VersionedMessage::V0(m) => {
                    insert_budget(&mut m.header, &mut m.account_keys, &mut m.instructions, fee);
                }
            }
        }

        let bh = self.latest_blockhash().await;
        match &mut msg {
            VersionedMessage::Legacy(m) => m.recent_blockhash = bh,
            VersionedMessage::V0(m) => m.recent_blockhash = bh,
        }
        let tx = VersionedTransaction::try_new(msg, &[&self.keypair])?;
        Ok(STANDARD.encode(serialize(&tx)?))
    }

    async fn send_batch(&self, txs: &[String]) -> Result<Vec<String>> {
        let mut sigs = Vec::new();
        for tx in txs {
            sigs.push(self.send_raw_tx(tx).await?);
        }
        Ok(sigs)
    }
}

fn change_ratio(a: f64, b: f64) -> f64 {
    if a == 0.0 && b == 0.0 {
        0.0
    } else {
        (a - b).abs() / a.abs().max(b.abs()).max(1.0)
    }
}

fn significant_change(old: &TokenAgg, new: &TokenAgg, th: f64) -> bool {
    let diff = change_ratio(old.bids, new.bids)
        .max(change_ratio(old.asks, new.asks))
        .max(change_ratio(old.tx_rate, new.tx_rate));
    diff > th
}

async fn update_mmap(
    token: &str,
    dex_map: &DexMap,
    mem: &MempoolMap,
    agg: &AggMap,
    adj: &AdjMap,
    adj_cache: &AdjacencyCache,
    mmap: &SharedMmap,
    offsets: &OffsetMap,
    next_off: &NextOffset,
    _ws: &WsSender,
    ws_bin: &WsBinSender,
    bus: Option<&mpsc::UnboundedSender<Vec<u8>>>,
    latest: &LatestSnap,
    latest_bin: &LatestBin,
    last_map: &LastAggMap,
    last_sent: &LastSent,
    threshold: f64,
    min_interval: Duration,
    diff_updates: bool,
    notify: Option<&tokio::sync::watch::Sender<()>>,
) -> Result<()> {
    let dexes = dex_map.lock().await;
    let dex_snapshot = dexes.clone();
    let mem_map = mem.lock().await;

    let adj_matrix = {
        let mut cache = adj_cache.lock().await;
        if let Some((prev, mat)) = cache.get(token) {
            if *prev == dex_snapshot {
                Some(mat.clone())
            } else {
                let res = route::build_adjacency_matrix(&dex_snapshot, token);
                if let Some(ref m) = res {
                    cache.insert(token.to_string(), (dex_snapshot.clone(), m.clone()));
                } else {
                    cache.remove(token);
                }
                res
            }
        } else {
            let res = route::build_adjacency_matrix(&dex_snapshot, token);
            if let Some(ref m) = res {
                cache.insert(token.to_string(), (dex_snapshot.clone(), m.clone()));
            }
            res
        }
    };

    let mut entry = TokenAgg::default();
    for (dex_name, dex) in dexes.iter() {
        if let Some(info) = dex.get(token) {
            entry.dex.insert(dex_name.clone(), info.clone());
            entry.bids += info.bids;
            entry.asks += info.asks;
        }
    }
    if let Some(rate) = mem_map.get(token) {
        entry.tx_rate = *rate;
    }
    entry.ts = Utc::now().timestamp_millis();

    drop(dexes);
    drop(mem_map);

    if let Some(adj_data) = adj_matrix.clone() {
        adj.lock().await.insert(token.to_string(), adj_data.clone());
    }

    let mut agg_lock = agg.lock().await;
    agg_lock.insert(token.to_string(), entry.clone());
    let snapshot = agg_lock.clone();
    let current_len = snapshot.len();
    drop(agg_lock);

    let now = Instant::now();
    let (send_needed, changed) = {
        let mut last = last_map.lock().await;
        let changed = match last.get(token) {
            Some(old) => significant_change(old, &entry, threshold),
            None => true,
        };
        if changed {
            last.insert(token.to_string(), entry.clone());
        }
        (changed || last.len() != current_len, changed)
    };

    // Serialize only the updated token in a compact binary format
    let options = bincode::options().with_fixint_encoding();
    let json = options.serialize(&entry)?;
    let adj_json = if let Some(ref adj_mat) = adj_matrix {
        Some(options.serialize(adj_mat)?)
    } else {
        None
    };
    let mut offsets_lock = offsets.lock().await;
    let mut next_lock = next_off.lock().await;
    let mut header_changed = false;
    let (offset, old_len) = match offsets_lock.get_mut(token) {
        Some((off, len)) => {
            let o = *off;
            if json.len() > *len {
                *off = *next_lock;
                *len = json.len();
                *next_lock += json.len();
                header_changed = true;
                (o, *len)
            } else {
                *len = (*len).max(json.len());
                (o, *len)
            }
        }
        None => {
            let off = *next_lock;
            offsets_lock.insert(token.to_string(), (off, json.len()));
            *next_lock += json.len();
            header_changed = true;
            (off, json.len())
        }
    };
    let adj_key = format!("adj_{}", token);
    let mut adj_offset = None;
    if let Some(buf) = adj_json.as_ref() {
        match offsets_lock.get_mut(&adj_key) {
            Some((off, len)) => {
                let o = *off;
                if buf.len() > *len {
                    *off = *next_lock;
                    *len = buf.len();
                    *next_lock += buf.len();
                    header_changed = true;
                    adj_offset = Some((o, *len));
                } else {
                    *len = (*len).max(buf.len());
                    adj_offset = Some((o, *len));
                }
            }
            None => {
                let off = *next_lock;
                offsets_lock.insert(adj_key.clone(), (off, buf.len()));
                *next_lock += buf.len();
                header_changed = true;
                adj_offset = Some((off, buf.len()));
            }
        }
    }
    if *next_lock > mmap.lock().await.len() {
        *next_lock = 0;
    }
    let header_size: usize = 8
        + offsets_lock
            .iter()
            .map(|(t, _)| 2 + t.as_bytes().len() + 8)
            .sum::<usize>();
    let mut mmap_lock = mmap.lock().await;
    if header_changed {
        let mut header = Vec::with_capacity(header_size);
        header.extend_from_slice(b"IDX1");
        header.extend_from_slice(&(offsets_lock.len() as u32).to_le_bytes());
        for (tok, (off, len)) in offsets_lock.iter() {
            header.extend_from_slice(&(tok.len() as u16).to_le_bytes());
            header.extend_from_slice(tok.as_bytes());
            header.extend_from_slice(&((header_size + *off) as u32).to_le_bytes());
            header.extend_from_slice(&(*len as u32).to_le_bytes());
        }
        let write_len = header.len().min(mmap_lock.len());
        mmap_lock[..write_len].copy_from_slice(&header[..write_len]);
    }
    let data_off = header_size + offset;
    if data_off + json.len() <= mmap_lock.len() {
        mmap_lock[data_off..data_off + json.len()].copy_from_slice(&json);
        for i in data_off + json.len()..data_off + old_len {
            if i < mmap_lock.len() {
                mmap_lock[i] = 0;
            }
        }
    }
    if let (Some(buf), Some((off, old))) = (adj_json.as_ref(), adj_offset) {
        let o = header_size + off;
        if o + buf.len() <= mmap_lock.len() {
            mmap_lock[o..o + buf.len()].copy_from_slice(buf);
            for i in o + buf.len()..o + old {
                if i < mmap_lock.len() {
                    mmap_lock[i] = 0;
                }
            }
        }
    }
    mmap_lock.flush()?;

    let binary = if send_needed || header_changed {
        // rebuild full snapshot for websocket clients
        let header_size: usize = 8
            + snapshot
                .iter()
                .map(|(t, _)| 2 + t.as_bytes().len() + 8)
                .sum::<usize>();
        let mut header = Vec::with_capacity(header_size);
        header.extend_from_slice(b"IDX1");
        header.extend_from_slice(&(snapshot.len() as u32).to_le_bytes());
        let mut data = Vec::new();
        let options = bincode::options().with_fixint_encoding();
        for (token, entry) in snapshot.iter() {
            let json = options.serialize(entry)?;
            header.extend_from_slice(&(token.len() as u16).to_le_bytes());
            header.extend_from_slice(token.as_bytes());
            header.extend_from_slice(&((header_size + data.len()) as u32).to_le_bytes());
            header.extend_from_slice(&(json.len() as u32).to_le_bytes());
            data.extend_from_slice(&json);
        }
        let mut buf = header;
        buf.extend_from_slice(&data);
        buf
    } else {
        Vec::new()
    };

    if !send_needed && now.duration_since(*last_sent.lock().await) < min_interval {
        return Ok(());
    }

    *last_sent.lock().await = now;

    // Build protobuf event for websocket clients and the event bus
    let entries: HashMap<String, pb::TokenAgg> = snapshot
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                pb::TokenAgg {
                    dex: v
                        .dex
                        .iter()
                        .map(|(dk, di)| {
                            (
                                dk.clone(),
                                pb::TokenInfo {
                                    bids: di.bids,
                                    asks: di.asks,
                                    tx_rate: di.tx_rate,
                                },
                            )
                        })
                        .collect(),
                    bids: v.bids,
                    asks: v.asks,
                    tx_rate: v.tx_rate,
                    ts: v.ts,
                },
            )
        })
        .collect();
    let schema_version_value = event_schema_version().to_string();
    let (event, send_binary) = if diff_updates && changed && !header_changed {
        let mut diff = HashMap::new();
        diff.insert(token.to_string(), entries.get(token).unwrap().clone());
        (
            pb::Event {
                topic: "depth_diff".to_string(),
                dedupe_key: token.to_string(),
                schema_version: schema_version_value.clone(),
                kind: Some(pb::event::Kind::DepthDiff(pb::DepthDiff { entries: diff })),
            },
            false,
        )
    } else {
        (
            pb::Event {
                topic: "depth_update".to_string(),
                dedupe_key: token.to_string(),
                schema_version: schema_version_value.clone(),
                kind: Some(pb::event::Kind::DepthUpdate(pb::DepthUpdate { entries })),
            },
            true,
        )
    };
    let proto = compress_event(&event.encode_to_vec());
    let _ = ws_bin.send(proto.clone());
    if send_binary {
        let _ = ws_bin.send(binary.clone());
    }
    *latest.lock().await = String::new();
    *latest_bin.lock().await = binary;
    if let Some(bus) = bus {
        let _ = bus.send(proto);
    }
    if let Some(tx) = notify {
        let _ = tx.send(());
    }
    Ok(())
}

async fn connect_feed(
    dex: &str,
    url: &str,
    dex_map: DexMap,
    mem: MempoolMap,
    agg: AggMap,
    adj: AdjMap,
    adj_cache: AdjacencyCache,
    mmap: SharedMmap,
    offsets: OffsetMap,
    next_off: NextOffset,
    ws: WsSender,
    ws_bin: WsBinSender,
    bus: Option<mpsc::UnboundedSender<Vec<u8>>>,
    latest: LatestSnap,
    latest_bin: LatestBin,
    last_map: LastAggMap,
    last_sent: LastSent,
    threshold: f64,
    min_interval: Duration,
    diff_updates: bool,
    notify: Option<tokio::sync::watch::Sender<()>>,
) -> Result<()> {
    let (socket, _) = connect_async(url).await?;
    let (_write, mut read) = socket.split();
    while let Some(Ok(msg)) = read.next().await {
        if !msg.is_text() {
            continue;
        }
        if let Ok(val) = serde_json::from_str::<Value>(&msg.to_string()) {
            if let (Some(token), Some(bids), Some(asks)) =
                (val.get("token"), val.get("bids"), val.get("asks"))
            {
                let token = token.as_str().unwrap_or("").to_string();
                let bids = bids.as_f64().unwrap_or(0.0);
                let asks = asks.as_f64().unwrap_or(0.0);
                let mut dexes = dex_map.lock().await;
                let entry = dexes.entry(dex.to_string()).or_default();
                entry.insert(
                    token.clone(),
                    TokenInfo {
                        bids,
                        asks,
                        tx_rate: 0.0,
                    },
                );
                drop(dexes);
                let _ = update_mmap(
                    &token,
                    &dex_map,
                    &mem,
                    &agg,
                    &adj,
                    &adj_cache,
                    &mmap,
                    &offsets,
                    &next_off,
                    &ws,
                    &ws_bin,
                    bus.as_ref(),
                    &latest,
                    &latest_bin,
                    &last_map,
                    &last_sent,
                    threshold,
                    min_interval,
                    diff_updates,
                    notify.as_ref(),
                )
                .await;
            }
        }
    }
    Ok(())
}

async fn connect_price_feed(
    dex: &str,
    url: &str,
    dex_map: DexMap,
    mem: MempoolMap,
    agg: AggMap,
    adj: AdjMap,
    adj_cache: AdjacencyCache,
    mmap: SharedMmap,
    offsets: OffsetMap,
    next_off: NextOffset,
    ws: WsSender,
    ws_bin: WsBinSender,
    bus: Option<mpsc::UnboundedSender<Vec<u8>>>,
    latest: LatestSnap,
    latest_bin: LatestBin,
    last_map: LastAggMap,
    last_sent: LastSent,
    threshold: f64,
    min_interval: Duration,
    diff_updates: bool,
    notify: Option<tokio::sync::watch::Sender<()>>,
) -> Result<()> {
    let (socket, _) = connect_async(url).await?;
    let (_write, mut read) = socket.split();
    while let Some(Ok(msg)) = read.next().await {
        if !msg.is_text() {
            continue;
        }
        if let Ok(val) = serde_json::from_str::<Value>(&msg.to_string()) {
            if let (Some(token), Some(price)) = (val.get("token"), val.get("price")) {
                let token = token.as_str().unwrap_or("").to_string();
                let price = price.as_f64().unwrap_or(0.0);
                let mut dexes = dex_map.lock().await;
                let entry = dexes.entry(dex.to_string()).or_default();
                entry.insert(
                    token.clone(),
                    TokenInfo {
                        bids: price,
                        asks: price,
                        tx_rate: 0.0,
                    },
                );
                drop(dexes);
                let _ = update_mmap(
                    &token,
                    &dex_map,
                    &mem,
                    &agg,
                    &adj,
                    &adj_cache,
                    &mmap,
                    &offsets,
                    &next_off,
                    &ws,
                    &ws_bin,
                    bus.as_ref(),
                    &latest,
                    &latest_bin,
                    &last_map,
                    &last_sent,
                    threshold,
                    min_interval,
                    diff_updates,
                    notify.as_ref(),
                )
                .await;
            }
        }
    }
    Ok(())
}

async fn connect_mempool(
    url: &str,
    mem: MempoolMap,
    dex_map: DexMap,
    agg: AggMap,
    adj: AdjMap,
    adj_cache: AdjacencyCache,
    mmap: SharedMmap,
    offsets: OffsetMap,
    next_off: NextOffset,
    ws: WsSender,
    ws_bin: WsBinSender,
    bus: Option<mpsc::UnboundedSender<Vec<u8>>>,
    latest: LatestSnap,
    latest_bin: LatestBin,
    last_map: LastAggMap,
    last_sent: LastSent,
    threshold: f64,
    min_interval: Duration,
    diff_updates: bool,
    notify: Option<tokio::sync::watch::Sender<()>>,
) -> Result<()> {
    let (socket, _) = connect_async(url).await?;
    let (_write, mut read) = socket.split();
    while let Some(Ok(msg)) = read.next().await {
        if !msg.is_text() {
            continue;
        }
        if let Ok(val) = serde_json::from_str::<Value>(&msg.to_string()) {
            if let (Some(token), Some(rate)) = (val.get("token"), val.get("tx_rate")) {
                let token = token.as_str().unwrap_or("").to_string();
                let rate = rate.as_f64().unwrap_or(0.0);
                let mut mem_lock = mem.lock().await;
                mem_lock.insert(token.clone(), rate);
                drop(mem_lock);
                let _ = update_mmap(
                    &token,
                    &dex_map,
                    &mem,
                    &agg,
                    &adj,
                    &adj_cache,
                    &mmap,
                    &offsets,
                    &next_off,
                    &ws,
                    &ws_bin,
                    bus.as_ref(),
                    &latest,
                    &latest_bin,
                    &last_map,
                    &last_sent,
                    threshold,
                    min_interval,
                    diff_updates,
                    notify.as_ref(),
                )
                .await;
            }
        }
    }
    Ok(())
}

async fn ipc_server(
    socket: &Path,
    dex_map: DexMap,
    mem: MempoolMap,
    agg: AggMap,
    adj: AdjMap,
    exec: Arc<ExecContext>,
    notify_rx: tokio::sync::watch::Receiver<()>,
) -> Result<()> {
    if socket.exists() {
        let _ = std::fs::remove_file(socket);
    }
    let listener = UnixListener::bind(socket)?;
    let auto_map: AutoExecMap = Arc::new(Mutex::new(HashMap::new()));

    {
        let d = dex_map.clone();
        let m = mem.clone();
        let exec = exec.clone();
        let a = auto_map.clone();
        let mut rx = notify_rx.clone();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                let entries = { a.lock().await.clone() };
                if entries.is_empty() {
                    continue;
                }
                let rates = { m.lock().await.clone() };
                let dexes = { d.lock().await.clone() };
                let mut remove = Vec::new();
                for (tok, cfg) in entries.iter() {
                    let rate = rates.get(tok).copied().unwrap_or(0.0);
                    let mut liq = 0.0;
                    for dex in dexes.values() {
                        if let Some(info) = dex.get(tok) {
                            liq += info.bids + info.asks;
                        }
                    }
                    if rate >= cfg.threshold || liq >= cfg.threshold {
                        for tx in &cfg.txs {
                            let _ = exec.submit_signed_tx(tx).await;
                        }
                        remove.push(tok.clone());
                    }
                }
                if !remove.is_empty() {
                    let mut lock = a.lock().await;
                    for t in remove {
                        lock.remove(&t);
                    }
                }
            }
        });
    }
    loop {
        let (mut stream, _) = listener.accept().await?;
        let dex_map = dex_map.clone();
        let _mem = mem.clone();
        let agg_map = agg.clone();
        let exec = exec.clone();
        let auto_map = auto_map.clone();
        let adj = adj.clone();
        tokio::spawn(async move {
            let mut buf = Vec::new();
            if stream.read_to_end(&mut buf).await.is_ok() {
                if let Ok(val) = serde_json::from_slice::<Value>(&buf) {
                    if val.get("cmd") == Some(&Value::String("snapshot".into())) {
                        if let Some(token) = val.get("token").and_then(|v| v.as_str()) {
                            let agg = agg_map.lock().await;
                            let entry = agg.get(token).cloned().unwrap_or_default();
                            let _ = stream
                                .write_all(serde_json::to_string(&entry).unwrap().as_bytes())
                                .await;
                        }
                    } else if val.get("cmd") == Some(&Value::String("signed_tx".into())) {
                        if let Some(tx) = val.get("tx").and_then(|v| v.as_str()) {
                            match exec.submit_signed_tx(tx).await {
                                Ok(sig) => {
                                    let _ = stream
                                        .write_all(
                                            format!("{{\"signature\":\"{}\"}}", sig).as_bytes(),
                                        )
                                        .await;
                                }
                                Err(e) => {
                                    let _ = stream
                                        .write_all(format!("{{\"error\":\"{}\"}}", e).as_bytes())
                                        .await;
                                }
                            }
                        }
                    } else if val.get("cmd") == Some(&Value::String("prepare".into())) {
                        if let Some(msg) = val.get("msg").and_then(|v| v.as_str()) {
                            let pf = val.get("priority_fee").and_then(|v| v.as_u64());
                            match exec.sign_template(msg, pf).await {
                                Ok(tx) => {
                                    let _ = stream
                                        .write_all(format!("{{\"tx\":\"{}\"}}", tx).as_bytes())
                                        .await;
                                }
                                Err(e) => {
                                    let _ = stream
                                        .write_all(format!("{{\"error\":\"{}\"}}", e).as_bytes())
                                        .await;
                                }
                            }
                        }
                    } else if val.get("cmd") == Some(&Value::String("batch".into())) {
                        if let Some(arr) = val.get("txs").and_then(|v| v.as_array()) {
                            let txs: Vec<String> = arr
                                .iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect();
                            match exec.send_batch(&txs).await {
                                Ok(sigs) => {
                                    let _ = stream
                                        .write_all(serde_json::to_string(&sigs).unwrap().as_bytes())
                                        .await;
                                }
                                Err(e) => {
                                    let _ = stream
                                        .write_all(format!("{{\"error\":\"{}\"}}", e).as_bytes())
                                        .await;
                                }
                            }
                        }
                    } else if val.get("cmd") == Some(&Value::String("raw_tx".into())) {
                        if let Some(tx) = val.get("tx").and_then(|v| v.as_str()) {
                            let pri =
                                val.get("priority_rpc")
                                    .and_then(|v| v.as_array())
                                    .map(|arr| {
                                        arr.iter()
                                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                            .collect::<Vec<String>>()
                                    });
                            match exec.send_raw_tx_priority(tx, pri).await {
                                Ok(sig) => {
                                    let _ = stream
                                        .write_all(
                                            format!("{{\"signature\":\"{}\"}}", sig).as_bytes(),
                                        )
                                        .await;
                                }
                                Err(e) => {
                                    let _ = stream
                                        .write_all(format!("{{\"error\":\"{}\"}}", e).as_bytes())
                                        .await;
                                }
                            }
                        }
                    } else if val.get("cmd") == Some(&Value::String("submit".into())) {
                        if let Some(tx) = val.get("tx").and_then(|v| v.as_str()) {
                            match exec.send_raw_tx(tx).await {
                                Ok(sig) => {
                                    let _ = stream
                                        .write_all(
                                            format!("{{\"signature\":\"{}\"}}", sig).as_bytes(),
                                        )
                                        .await;
                                }
                                Err(e) => {
                                    let _ = stream
                                        .write_all(format!("{{\"error\":\"{}\"}}", e).as_bytes())
                                        .await;
                                }
                            }
                        }
                    } else if val.get("cmd") == Some(&Value::String("order".into())) {
                        match exec.send_dummy_tx().await {
                            Ok(sig) => {
                                let _ = stream
                                    .write_all(format!("{{\"signature\":\"{}\"}}", sig).as_bytes())
                                    .await;
                            }
                            Err(_) => {
                                let _ = stream.write_all(b"{\"ok\":false}").await;
                            }
                        }
                    } else if val.get("cmd") == Some(&Value::String("auto_exec".into())) {
                        if let (Some(token), Some(th), Some(arr)) = (
                            val.get("token").and_then(|v| v.as_str()),
                            val.get("threshold").and_then(|v| v.as_f64()),
                            val.get("txs").and_then(|v| v.as_array()),
                        ) {
                            let txs: Vec<String> = arr
                                .iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect();
                            auto_map
                                .lock()
                                .await
                                .insert(token.to_string(), AutoExecEntry { threshold: th, txs });
                            let _ = stream.write_all(b"{\"ok\":true}").await;
                        }
                    } else if val.get("cmd") == Some(&Value::String("route".into())) {
                        if let (Some(token), Some(amount)) = (
                            val.get("token").and_then(|v| v.as_str()),
                            val.get("amount").and_then(|v| v.as_f64()),
                        ) {
                            let dexes = dex_map.lock().await;
                            let hops =
                                val.get("max_hops").and_then(|v| v.as_u64()).unwrap_or(4) as usize;
                            if let Some(res) = route::best_route(&dexes, token, amount, hops) {
                                let resp = serde_json::json!({
                                    "path": res.path,
                                    "profit": res.profit,
                                    "slippage": res.slippage
                                });
                                let _ = stream.write_all(resp.to_string().as_bytes()).await;
                            } else {
                                let _ = stream.write_all(b"{}\n").await;
                            }
                        }
                    } else if let Ok(req) = pb::RouteRequest::decode(&*buf) {
                        let hops = if req.max_hops == 0 { 4 } else { req.max_hops as usize };
                        let adj_map = adj.lock().await;
                        if let Some(adj_mat) = adj_map.get(&req.token) {
                            if let Some(res) = route::best_route_matrix(adj_mat, req.amount, hops) {
                                let resp = pb::RouteResponse { path: res.path, profit: res.profit, slippage: res.slippage };
                                let _ = stream.write_all(&resp.encode_to_vec()).await;
                            } else {
                                let resp = pb::RouteResponse::default();
                                let _ = stream.write_all(&resp.encode_to_vec()).await;
                            }
                        } else {
                            let resp = pb::RouteResponse::default();
                            let _ = stream.write_all(&resp.encode_to_vec()).await;
                        }
                    }
                }
            }
            let _ = stream.shutdown().await;
        });
    }
}

async fn ws_server(
    addr: &str,
    port: u16,
    tx: WsSender,
    bin_tx: WsBinSender,
    latest: LatestSnap,
    latest_bin: LatestBin,
) -> Result<()> {
    let listener = TcpListener::bind((addr, port)).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let tx = tx.clone();
        let bin_tx = bin_tx.clone();
        let latest = latest.clone();
        let latest_bin = latest_bin.clone();
        tokio::spawn(async move {
            if let Ok(ws) = accept_async(stream).await {
                let (mut write, _) = ws.split();
                let snapshot = latest.lock().await.clone();
                if !snapshot.is_empty() {
                    let _ = write.send(WsMessage::Text(snapshot)).await;
                }
                let snap_bin = latest_bin.lock().await.clone();
                if !snap_bin.is_empty() {
                    let _ = write.send(WsMessage::Binary(snap_bin)).await;
                }
                let mut rx = tx.subscribe();
                let mut rx_bin = bin_tx.subscribe();
                loop {
                    tokio::select! {
                        Ok(msg) = rx.recv() => {
                            if write.send(WsMessage::Text(msg)).await.is_err() {
                                break;
                            }
                        }
                        Ok(msg) = rx_bin.recv() => {
                            if write.send(WsMessage::Binary(msg)).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mut config_path = None;
    let mut serum = None;
    let mut raydium = None;
    let mut orca = None;
    let mut jupiter = None;
    let mut phoenix = None;
    let mut meteora = None;
    let mut mempool = None;
    let mut rpc = std::env::var("SOLANA_RPC_URL").unwrap_or_default();
    let mut keypair_path = std::env::var("SOLANA_KEYPAIR").unwrap_or_default();
    for w in args.windows(2) {
        match w[0].as_str() {
            "--config" => config_path = Some(w[1].clone()),
            "--serum" => serum = Some(w[1].clone()),
            "--raydium" => raydium = Some(w[1].clone()),
            "--rpc" => rpc = w[1].clone(),
            "--keypair" => keypair_path = w[1].clone(),
            "--mempool" => mempool = Some(w[1].clone()),
            "--orca" => orca = Some(w[1].clone()),
            "--jupiter" => jupiter = Some(w[1].clone()),
            "--phoenix" => phoenix = Some(w[1].clone()),
            "--meteora" => meteora = Some(w[1].clone()),
            _ => {}
        }
    }
    let cfg = if let Some(path) = config_path {
        load_config(&path).unwrap_or_default()
    } else {
        TomlTable::new()
    };
    if serum.is_none() {
        serum = std::env::var("SERUM_WS_URL")
            .ok()
            .or_else(|| cfg_str(&cfg, "serum_ws_url"));
    }
    if raydium.is_none() {
        raydium = std::env::var("RAYDIUM_WS_URL")
            .ok()
            .or_else(|| cfg_str(&cfg, "raydium_ws_url"));
    }
    if orca.is_none() {
        orca = std::env::var("ORCA_WS_URL")
            .ok()
            .or_else(|| cfg_str(&cfg, "orca_ws_url"));
    }
    if jupiter.is_none() {
        jupiter = std::env::var("JUPITER_WS_URL")
            .ok()
            .or_else(|| cfg_str(&cfg, "jupiter_ws_url"));
    }
    if phoenix.is_none() {
        phoenix = std::env::var("PHOENIX_WS_URL")
            .ok()
            .or_else(|| cfg_str(&cfg, "phoenix_ws_url"));
    }
    if meteora.is_none() {
        meteora = std::env::var("METEORA_WS_URL")
            .ok()
            .or_else(|| cfg_str(&cfg, "meteora_ws_url"));
    }
    if mempool.is_none() {
        mempool = std::env::var("MEMPOOL_WS_URL")
            .ok()
            .or_else(|| cfg_str(&cfg, "mempool_ws_url"));
    }
    if rpc.is_empty() {
        if let Ok(env_rpc) = std::env::var("SOLANA_RPC_URL") {
            rpc = env_rpc;
        } else if let Some(val) = cfg_str(&cfg, "solana_rpc_url") {
            rpc = val;
        }
    }
    if keypair_path.is_empty() {
        if let Ok(env_kp) = std::env::var("SOLANA_KEYPAIR") {
            keypair_path = env_kp;
        } else if let Some(val) = cfg_str(&cfg, "solana_keypair") {
            keypair_path = val;
        }
    }
    let mmap_path = std::env::var("DEPTH_MMAP_PATH")
        .ok()
        .or_else(|| cfg_str(&cfg, "DEPTH_MMAP_PATH"))
        .unwrap_or_else(|| "/tmp/depth_service.mmap".into());
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&mmap_path)?;
    file.set_len(65536)?;
    let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
    let mmap = Arc::new(Mutex::new(mmap));
    let offsets: OffsetMap = Arc::new(Mutex::new(HashMap::new()));
    let next_off: NextOffset = Arc::new(Mutex::new(0usize));
    let dex_map: DexMap = Arc::new(Mutex::new(HashMap::new()));
    let mem_map: MempoolMap = Arc::new(Mutex::new(HashMap::new()));
    let agg_map: AggMap = Arc::new(Mutex::new(HashMap::new()));
    let adj_map: AdjMap = Arc::new(Mutex::new(HashMap::new()));
    let adj_cache: AdjacencyCache = Arc::new(Mutex::new(HashMap::new()));
    let (ws_tx, _) = broadcast::channel(16);
    let (ws_bin_tx, _) = broadcast::channel(16);
    let latest: LatestSnap = Arc::new(Mutex::new(String::new()));
    let latest_bin: LatestBin = Arc::new(Mutex::new(Vec::new()));
    let last_map: LastAggMap = Arc::new(Mutex::new(HashMap::new()));
    let min_interval = send_interval(&cfg);
    let threshold = update_threshold(&cfg);
    let diff_updates_flag = diff_updates(&cfg);
    let hb_interval = heartbeat_interval(&cfg);
    let last_sent: LastSent = Arc::new(Mutex::new(Instant::now() - min_interval));
    let (notify_tx, notify_rx) = tokio::sync::watch::channel(());
    let bus_tx = if let Some(url) = event_bus_url(&cfg) {
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                match connect_async(&url).await {
                    Ok((socket, _)) => {
                        backoff = Duration::from_secs(1);
                        let (write, mut read) = socket.split();
                        let write = Arc::new(Mutex::new(write));
                        let write_read = write.clone();
                        let schema_version_value = event_schema_version().to_string();
                        // Notify bus that the service is online
                        let online = pb::Event {
                            topic: "depth_service_status".to_string(),
                            dedupe_key: "depth_service_status".to_string(),
                            schema_version: schema_version_value.clone(),
                            kind: Some(pb::event::Kind::DepthServiceStatus(
                                pb::DepthServiceStatus {
                                    status: "online".to_string(),
                                },
                            )),
                        };
                        let _ = write
                            .lock()
                            .await
                            .send(WsMessage::Binary(compress_event(&online.encode_to_vec())))
                            .await;
                        let tx_metrics = tx_clone.clone();
                        let tx_hb = tx_clone.clone();
                        let schema_version_metrics = schema_version_value.clone();
                        tokio::spawn(async move {
                            let mut sys = System::new();
                            loop {
                                sys.refresh_cpu();
                                sys.refresh_memory();
                                let cpu = sys.global_cpu_info().cpu_usage() as f64;
                                let mem = if sys.total_memory() > 0 {
                                    sys.used_memory() as f64 * 100.0 / sys.total_memory() as f64
                                } else { 0.0 };
                                let ev = pb::Event {
                                    topic: "system_metrics".to_string(),
                                    dedupe_key: "system_metrics".to_string(),
                                    schema_version: schema_version_metrics.clone(),
                                    kind: Some(pb::event::Kind::SystemMetrics(pb::SystemMetrics {
                                        cpu,
                                        memory: mem,
                                    })),
                                };
                                if tx_metrics.send(ev.encode_to_vec()).is_err() {
                                    break;
                                }
                                tokio::time::sleep(Duration::from_secs(30)).await;
                            }
                        });
                        let schema_version_hb = schema_version_value.clone();
                        tokio::spawn(async move {
                            loop {
                                let ev = pb::Event {
                                    topic: "heartbeat".to_string(),
                                    dedupe_key: "heartbeat:depth_service".to_string(),
                                    schema_version: schema_version_hb.clone(),
                                    kind: Some(pb::event::Kind::Heartbeat(pb::Heartbeat {
                                        service: "depth_service".to_string(),
                                    })),
                                };
                                if tx_hb.send(ev.encode_to_vec()).is_err() {
                                    break;
                                }
                                tokio::time::sleep(hb_interval).await;
                            }
                        });
                        tokio::spawn(async move {
                            while let Some(Ok(msg)) = read.next().await {
                                if let WsMessage::Binary(data) = msg {
                                    let decoded = maybe_decompress(&data);
                                    if let Ok(ev) = pb::Event::decode(&*decoded) {
                                        if ev.topic == "heartbeat" {
                                            let _ = write_read
                                                .lock()
                                                .await
                                                .send(WsMessage::Binary(compress_event(&decoded)))
                                                .await;
                                        }
                                    }
                                }
                            }
                        });
                        while let Some(msg) = rx.recv().await {
                            if write
                                .lock()
                                .await
                                .send(WsMessage::Binary(compress_event(&msg)))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(64));
            }
        });
        Some(tx)
    } else {
        None
    };
    let (ws_addr, ws_port) = depth_ws_addr(&cfg);
    {
        let tx = ws_tx.clone();
        let bin_tx = ws_bin_tx.clone();
        let addr = ws_addr.clone();
        let latest_snap = latest.clone();
        let latest_bin_snap = latest_bin.clone();
        tokio::spawn(async move {
            let _ = ws_server(&addr, ws_port, tx, bin_tx, latest_snap, latest_bin_snap).await;
        });
    }

    if let Some(url) = serum {
        let d = dex_map.clone();
        let m = mem_map.clone();
        let a = agg_map.clone();
        let mm = mmap.clone();
        let tx = ws_tx.clone();
        let bin_tx = ws_bin_tx.clone();
        let bus = bus_tx.clone();
        let off = offsets.clone();
        let next = next_off.clone();
        let latest = latest.clone();
        let latest_bin_c = latest_bin.clone();
        let last_map_c = last_map.clone();
        let last_sent_c = last_sent.clone();
        let adj_map = adj_map.clone();
        let cache = adj_cache.clone();
        let notify = notify_tx.clone();
        tokio::spawn(async move {
            let _ = connect_feed(
                "serum",
                &url,
                d,
                m,
                a,
                adj_map.clone(),
                cache,
                mm,
                off,
                next,
                tx,
                bin_tx,
                bus,
                latest,
                latest_bin_c,
                last_map_c,
                last_sent_c,
                threshold,
                min_interval,
                diff_updates_flag,
                Some(notify),
            )
            .await;
        });
    }
    if let Some(url) = raydium {
        let d = dex_map.clone();
        let m = mem_map.clone();
        let a = agg_map.clone();
        let mm = mmap.clone();
        let tx = ws_tx.clone();
        let bin_tx = ws_bin_tx.clone();
        let bus = bus_tx.clone();
        let off = offsets.clone();
        let next = next_off.clone();
        let latest = latest.clone();
        let latest_bin_c = latest_bin.clone();
        let last_map_c = last_map.clone();
        let last_sent_c = last_sent.clone();
        let adj_map = adj_map.clone();
        let cache = adj_cache.clone();
        let notify = notify_tx.clone();
        tokio::spawn(async move {
            let _ = connect_price_feed(
                "raydium",
                &url,
                d,
                m,
                a,
                adj_map.clone(),
                cache,
                mm,
                off,
                next,
                tx,
                bin_tx,
                bus,
                latest,
                latest_bin_c,
                last_map_c,
                last_sent_c,
                threshold,
                min_interval,
                diff_updates_flag,
                Some(notify),
            )
            .await;
        });
    }
    if let Some(url) = orca {
        let d = dex_map.clone();
        let m = mem_map.clone();
        let a = agg_map.clone();
        let mm = mmap.clone();
        let tx = ws_tx.clone();
        let bin_tx = ws_bin_tx.clone();
        let bus = bus_tx.clone();
        let off = offsets.clone();
        let next = next_off.clone();
        let latest = latest.clone();
        let latest_bin_c = latest_bin.clone();
        let last_map_c = last_map.clone();
        let last_sent_c = last_sent.clone();
        let adj_map = adj_map.clone();
        let cache = adj_cache.clone();
        let notify = notify_tx.clone();
        tokio::spawn(async move {
            let _ = connect_price_feed(
                "orca",
                &url,
                d,
                m,
                a,
                adj_map.clone(),
                cache,
                mm,
                off,
                next,
                tx,
                bin_tx,
                bus,
                latest,
                latest_bin_c,
                last_map_c,
                last_sent_c,
                threshold,
                min_interval,
                diff_updates_flag,
                Some(notify),
            )
            .await;
        });
    }
    if let Some(url) = jupiter {
        let d = dex_map.clone();
        let m = mem_map.clone();
        let a = agg_map.clone();
        let mm = mmap.clone();
        let tx = ws_tx.clone();
        let bin_tx = ws_bin_tx.clone();
        let bus = bus_tx.clone();
        let off = offsets.clone();
        let next = next_off.clone();
        let latest = latest.clone();
        let latest_bin_c = latest_bin.clone();
        let last_map_c = last_map.clone();
        let last_sent_c = last_sent.clone();
        let adj_map = adj_map.clone();
        let cache = adj_cache.clone();
        let notify = notify_tx.clone();
        tokio::spawn(async move {
            let _ = connect_price_feed(
                "jupiter",
                &url,
                d,
                m,
                a,
                adj_map.clone(),
                cache,
                mm,
                off,
                next,
                tx,
                bin_tx,
                bus,
                latest,
                latest_bin_c,
                last_map_c,
                last_sent_c,
                threshold,
                min_interval,
                diff_updates_flag,
                Some(notify),
            )
            .await;
        });
    }
    if let Some(url) = phoenix {
        let d = dex_map.clone();
        let m = mem_map.clone();
        let a = agg_map.clone();
        let mm = mmap.clone();
        let tx = ws_tx.clone();
        let bin_tx = ws_bin_tx.clone();
        let bus = bus_tx.clone();
        let off = offsets.clone();
        let next = next_off.clone();
        let latest = latest.clone();
        let latest_bin_c = latest_bin.clone();
        let last_map_c = last_map.clone();
        let last_sent_c = last_sent.clone();
        let adj_map = adj_map.clone();
        let cache = adj_cache.clone();
        let notify = notify_tx.clone();
        tokio::spawn(async move {
            let _ = connect_price_feed(
                "phoenix",
                &url,
                d,
                m,
                a,
                adj_map.clone(),
                cache,
                mm,
                off,
                next,
                tx,
                bin_tx,
                bus,
                latest,
                latest_bin_c,
                last_map_c,
                last_sent_c,
                threshold,
                min_interval,
                diff_updates_flag,
                Some(notify),
            )
            .await;
        });
    }
    if let Some(url) = meteora {
        let d = dex_map.clone();
        let m = mem_map.clone();
        let a = agg_map.clone();
        let mm = mmap.clone();
        let tx = ws_tx.clone();
        let bin_tx = ws_bin_tx.clone();
        let bus = bus_tx.clone();
        let off = offsets.clone();
        let next = next_off.clone();
        let latest = latest.clone();
        let latest_bin_c = latest_bin.clone();
        let last_map_c = last_map.clone();
        let last_sent_c = last_sent.clone();
        let adj_map = adj_map.clone();
        let cache = adj_cache.clone();
        let notify = notify_tx.clone();
        tokio::spawn(async move {
            let _ = connect_price_feed(
                "meteora",
                &url,
                d,
                m,
                a,
                adj_map.clone(),
                cache,
                mm,
                off,
                next,
                tx,
                bin_tx,
                bus,
                latest,
                latest_bin_c,
                last_map_c,
                last_sent_c,
                threshold,
                min_interval,
                diff_updates_flag,
                Some(notify),
            )
            .await;
        });
    }
    if let Some(url) = mempool {
        let d = dex_map.clone();
        let m = mem_map.clone();
        let a = agg_map.clone();
        let mm = mmap.clone();
        let tx = ws_tx.clone();
        let bin_tx = ws_bin_tx.clone();
        let bus = bus_tx.clone();
        let off = offsets.clone();
        let next = next_off.clone();
        let latest = latest.clone();
        let latest_bin_c = latest_bin.clone();
        let last_map_c = last_map.clone();
        let last_sent_c = last_sent.clone();
        let adj_map = adj_map.clone();
        let cache = adj_cache.clone();
        let notify = notify_tx.clone();
        tokio::spawn(async move {
            let _ = connect_mempool(
                &url,
                m,
                d,
                a,
                adj_map.clone(),
                cache,
                mm,
                off,
                next,
                tx,
                bin_tx,
                bus,
                latest,
                latest_bin_c,
                last_map_c,
                last_sent_c,
                threshold,
                min_interval,
                diff_updates_flag,
                Some(notify),
            )
            .await;
        });
    }

    let paper_mode = std::env::var("MODE")
        .ok()
        .map(|mode| mode.eq_ignore_ascii_case("paper"))
        .unwrap_or(false);
    let kp = if !keypair_path.is_empty() {
        match read_keypair_file(&keypair_path) {
            Ok(kp) => kp,
            Err(_err) if paper_mode => {
                if PAPER_KEYPAIR_WARNED.set(()).is_ok() {
                    eprintln!(
                        "[depth_service] keypair missing at {} (paper mode); continuing with ephemeral key",
                        keypair_path
                    );
                }
                Keypair::new()
            }
            Err(err) => {
                return Err(anyhow!("failed to read keypair {}: {}", keypair_path, err));
            }
        }
    } else {
        Keypair::new()
    };
    if rpc.is_empty() {
        rpc = "https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY".to_string();
    }
    let exec = Arc::new(ExecContext::new(&rpc, kp).await);

    let sock_path = std::env::var("DEPTH_SERVICE_SOCKET")
        .ok()
        .or_else(|| cfg_str(&cfg, "DEPTH_SERVICE_SOCKET"))
        .unwrap_or_else(|| "/tmp/depth_service.sock".into());
    ipc_server(
        Path::new(&sock_path),
        dex_map,
        mem_map,
        agg_map,
        adj_map,
        exec,
        notify_rx,
    )
    .await?;
    Ok(())
}
