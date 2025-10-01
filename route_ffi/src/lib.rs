#[cfg(feature = "parallel")]
use rayon::{prelude::*, ThreadPoolBuilder};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
#[cfg(feature = "parallel")]
use std::sync::Once;

#[derive(Clone)]
struct Node {
    neg_profit: f64,
    path: Vec<String>,
    visited: HashSet<String>,
}

impl Eq for Node {}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.neg_profit == other.neg_profit
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .neg_profit
            .partial_cmp(&self.neg_profit)
            .unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
use bincode;
use bincode::Options;
use libc::c_char;
use serde::{Deserialize, Serialize};
use std::ffi::{CStr, CString};

fn parse_map_bin(ptr: *const u8, len: usize) -> Option<HashMap<String, f64>> {
    if ptr.is_null() || len == 0 {
        return Some(HashMap::new());
    }
    let buf = unsafe { std::slice::from_raw_parts(ptr, len) };
    let opts = bincode::options().with_fixint_encoding();
    opts.deserialize(buf).ok()
}

fn parse_map(ptr: *const c_char) -> Option<HashMap<String, f64>> {
    if ptr.is_null() {
        return Some(HashMap::new());
    }
    let s = unsafe { CStr::from_ptr(ptr) }.to_str().ok()?;
    serde_json::from_str::<HashMap<String, f64>>(s).ok()
}

#[cfg(feature = "parallel")]
static INIT: Once = Once::new();

#[cfg(feature = "parallel")]
fn init_thread_pool() {
    INIT.call_once(|| {
        if let Ok(var) = std::env::var("RAYON_NUM_THREADS") {
            if let Ok(n) = var.parse::<usize>() {
                let _ = ThreadPoolBuilder::new().num_threads(n).build_global();
            }
        }
    });
}

fn best_route_internal(
    prices: &HashMap<String, f64>,
    amount: f64,
    fees: &HashMap<String, f64>,
    gas: &HashMap<String, f64>,
    latency: &HashMap<String, f64>,
    max_hops: usize,
) -> Option<(Vec<String>, f64)> {
    if prices.len() < 2 || max_hops < 2 {
        return None;
    }
    let trade_amount = amount;
    let venues: Vec<String> = prices.keys().cloned().collect();
    let mut adjacency: HashMap<String, HashMap<String, f64>> = HashMap::new();
    for a in venues.iter() {
        let mut neigh = HashMap::new();
        for b in venues.iter() {
            if a == b {
                continue;
            }
            let pa = *prices.get(a).unwrap_or(&0.0);
            let pb = *prices.get(b).unwrap_or(&0.0);
            let cost = pa * trade_amount * fees.get(a).copied().unwrap_or(0.0)
                + pb * trade_amount * fees.get(b).copied().unwrap_or(0.0)
                + gas.get(a).copied().unwrap_or(0.0)
                + gas.get(b).copied().unwrap_or(0.0)
                + latency.get(a).copied().unwrap_or(0.0)
                + latency.get(b).copied().unwrap_or(0.0);
            let profit = (pb - pa) * trade_amount - cost;
            neigh.insert(b.clone(), profit);
        }
        adjacency.insert(a.clone(), neigh);
    }
    let mut best_path: Vec<String> = Vec::new();
    let mut best_profit = f64::NEG_INFINITY;
    let mut heap: BinaryHeap<Node> = BinaryHeap::new();
    for v in venues.iter() {
        let visited: HashSet<String> = std::iter::once(v.clone()).collect();
        heap.push(Node {
            neg_profit: 0.0,
            path: vec![v.clone()],
            visited,
        });
    }
    while let Some(Node {
        neg_profit,
        path,
        visited,
    }) = heap.pop()
    {
        let profit = -neg_profit;
        if path.len() > 1 && profit > best_profit {
            best_profit = profit;
            best_path = path.clone();
        }
        if path.len() >= max_hops {
            continue;
        }
        let last = path.last().unwrap().clone();
        if let Some(neigh) = adjacency.get(&last) {
            for (nxt, val) in neigh {
                if visited.contains(nxt) {
                    continue;
                }
                let mut new_path = path.clone();
                new_path.push(nxt.clone());
                let mut new_vis = visited.clone();
                new_vis.insert(nxt.clone());
                let new_profit = profit + val;
                heap.push(Node {
                    neg_profit: -new_profit,
                    path: new_path,
                    visited: new_vis,
                });
            }
        }
    }
    if best_profit.is_finite() {
        Some((best_path, best_profit))
    } else {
        None
    }
}

#[cfg(feature = "parallel")]
fn search_from_start(
    start: &str,
    adjacency: &HashMap<String, HashMap<String, f64>>,
    max_hops: usize,
) -> (Vec<String>, f64) {
    let mut best_path: Vec<String> = Vec::new();
    let mut best_profit = f64::NEG_INFINITY;
    let mut heap: BinaryHeap<Node> = BinaryHeap::new();
    let visited: HashSet<String> = std::iter::once(start.to_string()).collect();
    heap.push(Node {
        neg_profit: 0.0,
        path: vec![start.to_string()],
        visited,
    });
    while let Some(Node {
        neg_profit,
        path,
        visited,
    }) = heap.pop()
    {
        let profit = -neg_profit;
        if path.len() > 1 && profit > best_profit {
            best_profit = profit;
            best_path = path.clone();
        }
        if path.len() >= max_hops {
            continue;
        }
        let last = path.last().unwrap().clone();
        if let Some(neigh) = adjacency.get(&last) {
            for (nxt, val) in neigh {
                if visited.contains(nxt) {
                    continue;
                }
                let mut new_path = path.clone();
                new_path.push(nxt.clone());
                let mut new_vis = visited.clone();
                new_vis.insert(nxt.clone());
                let new_profit = profit + val;
                heap.push(Node {
                    neg_profit: -new_profit,
                    path: new_path,
                    visited: new_vis,
                });
            }
        }
    }
    (best_path, best_profit)
}

#[cfg(feature = "parallel")]
fn best_route_parallel(
    prices: &HashMap<String, f64>,
    amount: f64,
    fees: &HashMap<String, f64>,
    gas: &HashMap<String, f64>,
    latency: &HashMap<String, f64>,
    max_hops: usize,
) -> Option<(Vec<String>, f64)> {
    if prices.len() < 2 || max_hops < 2 {
        return None;
    }
    let trade_amount = amount;
    let venues: Vec<String> = prices.keys().cloned().collect();
    let mut adjacency: HashMap<String, HashMap<String, f64>> = HashMap::new();
    for a in venues.iter() {
        let mut neigh = HashMap::new();
        for b in venues.iter() {
            if a == b {
                continue;
            }
            let pa = *prices.get(a).unwrap_or(&0.0);
            let pb = *prices.get(b).unwrap_or(&0.0);
            let cost = pa * trade_amount * fees.get(a).copied().unwrap_or(0.0)
                + pb * trade_amount * fees.get(b).copied().unwrap_or(0.0)
                + gas.get(a).copied().unwrap_or(0.0)
                + gas.get(b).copied().unwrap_or(0.0)
                + latency.get(a).copied().unwrap_or(0.0)
                + latency.get(b).copied().unwrap_or(0.0);
            let profit = (pb - pa) * trade_amount - cost;
            neigh.insert(b.clone(), profit);
        }
        adjacency.insert(a.clone(), neigh);
    }
    venues
        .par_iter()
        .map(|v| search_from_start(v, &adjacency, max_hops))
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal))
}

#[no_mangle]
pub extern "C" fn best_route_json(
    prices_json: *const c_char,
    fees_json: *const c_char,
    gas_json: *const c_char,
    latency_json: *const c_char,
    amount: f64,
    max_hops: u32,
    out_profit: *mut f64,
) -> *mut c_char {
    let prices = match parse_map(prices_json) {
        Some(m) => m,
        None => return std::ptr::null_mut(),
    };
    let fees = parse_map(fees_json).unwrap_or_default();
    let gas = parse_map(gas_json).unwrap_or_default();
    let latency = parse_map(latency_json).unwrap_or_default();
    match best_route_internal(&prices, amount, &fees, &gas, &latency, max_hops as usize) {
        Some((path, profit)) => unsafe {
            *out_profit = profit;
            let s = serde_json::to_string(&path).unwrap_or_else(|_| "[]".to_string());
            CString::new(s).unwrap().into_raw()
        },
        None => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn search_route_json(
    prices_json: *const c_char,
    fees_json: *const c_char,
    gas_json: *const c_char,
    latency_json: *const c_char,
    amount: f64,
    max_hops: u32,
    out_profit: *mut f64,
) -> *mut c_char {
    let prices = match parse_map(prices_json) {
        Some(m) => m,
        None => return std::ptr::null_mut(),
    };
    let fees = parse_map(fees_json).unwrap_or_default();
    let gas = parse_map(gas_json).unwrap_or_default();
    let latency = parse_map(latency_json).unwrap_or_default();
    match best_route_internal(&prices, amount, &fees, &gas, &latency, max_hops as usize) {
        Some((path, profit)) => unsafe {
            *out_profit = profit;
            let s = serde_json::to_string(&path).unwrap_or_else(|_| "[]".to_string());
            CString::new(s).unwrap().into_raw()
        },
        None => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn best_route_parallel_json(
    prices_json: *const c_char,
    fees_json: *const c_char,
    gas_json: *const c_char,
    latency_json: *const c_char,
    amount: f64,
    max_hops: u32,
    out_profit: *mut f64,
) -> *mut c_char {
    #[cfg(feature = "parallel")]
    init_thread_pool();
    let prices = match parse_map(prices_json) {
        Some(m) => m,
        None => return std::ptr::null_mut(),
    };
    let fees = parse_map(fees_json).unwrap_or_default();
    let gas = parse_map(gas_json).unwrap_or_default();
    let latency = parse_map(latency_json).unwrap_or_default();
    #[cfg(feature = "parallel")]
    let result = best_route_parallel(&prices, amount, &fees, &gas, &latency, max_hops as usize);
    #[cfg(not(feature = "parallel"))]
    let result = best_route_internal(&prices, amount, &fees, &gas, &latency, max_hops as usize);
    match result {
        Some((path, profit)) => unsafe {
            *out_profit = profit;
            let s = serde_json::to_string(&path).unwrap_or_else(|_| "[]".to_string());
            CString::new(s).unwrap().into_raw()
        },
        None => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn free_cstring(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        drop(CString::from_raw(ptr));
    }
}

#[no_mangle]
pub extern "C" fn route_parallel_enabled() -> bool {
    cfg!(feature = "parallel")
}

#[derive(Serialize, Deserialize)]
struct TokenInfo {
    bids: f64,
    asks: f64,
    tx_rate: f64,
}

#[derive(Serialize, Deserialize)]
struct TokenAgg {
    dex: HashMap<String, TokenInfo>,
    bids: f64,
    asks: f64,
    tx_rate: f64,
    ts: i64,
}

#[no_mangle]
pub extern "C" fn decode_token_agg_json(data: *const u8, len: usize) -> *mut c_char {
    if data.is_null() || len == 0 {
        return std::ptr::null_mut();
    }
    let buf = unsafe { std::slice::from_raw_parts(data, len) };
    let opts = bincode::options().with_fixint_encoding();
    let agg: TokenAgg = match opts.deserialize(buf) {
        Ok(v) => v,
        Err(_) => return std::ptr::null_mut(),
    };
    let s = serde_json::to_string(&agg).unwrap_or_else(|_| "{}".to_string());
    CString::new(s).unwrap().into_raw()
}

#[no_mangle]
pub extern "C" fn free_buffer(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    unsafe {
        Vec::from_raw_parts(ptr, len, len);
    }
}

#[no_mangle]
pub extern "C" fn best_route_bin(
    prices_ptr: *const u8,
    prices_len: usize,
    fees_ptr: *const u8,
    fees_len: usize,
    gas_ptr: *const u8,
    gas_len: usize,
    latency_ptr: *const u8,
    latency_len: usize,
    amount: f64,
    max_hops: u32,
    out_profit: *mut f64,
    out_len: *mut usize,
) -> *mut u8 {
    let prices = match parse_map_bin(prices_ptr, prices_len) {
        Some(m) => m,
        None => return std::ptr::null_mut(),
    };
    let fees = parse_map_bin(fees_ptr, fees_len).unwrap_or_default();
    let gas = parse_map_bin(gas_ptr, gas_len).unwrap_or_default();
    let latency = parse_map_bin(latency_ptr, latency_len).unwrap_or_default();
    match best_route_internal(&prices, amount, &fees, &gas, &latency, max_hops as usize) {
        Some((path, profit)) => unsafe {
            *out_profit = profit;
            let opts = bincode::options().with_fixint_encoding();
            let mut buf = opts.serialize(&path).unwrap_or_default();
            let len = buf.len();
            let ptr = buf.as_mut_ptr();
            std::mem::forget(buf);
            if !out_len.is_null() {
                *out_len = len;
            }
            ptr
        },
        None => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn search_route_bin(
    prices_ptr: *const u8,
    prices_len: usize,
    fees_ptr: *const u8,
    fees_len: usize,
    gas_ptr: *const u8,
    gas_len: usize,
    latency_ptr: *const u8,
    latency_len: usize,
    amount: f64,
    max_hops: u32,
    out_profit: *mut f64,
    out_len: *mut usize,
) -> *mut u8 {
    best_route_bin(
        prices_ptr,
        prices_len,
        fees_ptr,
        fees_len,
        gas_ptr,
        gas_len,
        latency_ptr,
        latency_len,
        amount,
        max_hops,
        out_profit,
        out_len,
    )
}

#[no_mangle]
pub extern "C" fn best_route_parallel_bin(
    prices_ptr: *const u8,
    prices_len: usize,
    fees_ptr: *const u8,
    fees_len: usize,
    gas_ptr: *const u8,
    gas_len: usize,
    latency_ptr: *const u8,
    latency_len: usize,
    amount: f64,
    max_hops: u32,
    out_profit: *mut f64,
    out_len: *mut usize,
) -> *mut u8 {
    #[cfg(feature = "parallel")]
    init_thread_pool();
    let prices = match parse_map_bin(prices_ptr, prices_len) {
        Some(m) => m,
        None => return std::ptr::null_mut(),
    };
    let fees = parse_map_bin(fees_ptr, fees_len).unwrap_or_default();
    let gas = parse_map_bin(gas_ptr, gas_len).unwrap_or_default();
    let latency = parse_map_bin(latency_ptr, latency_len).unwrap_or_default();
    #[cfg(feature = "parallel")]
    let result = best_route_parallel(&prices, amount, &fees, &gas, &latency, max_hops as usize);
    #[cfg(not(feature = "parallel"))]
    let result = best_route_internal(&prices, amount, &fees, &gas, &latency, max_hops as usize);
    match result {
        Some((path, profit)) => unsafe {
            *out_profit = profit;
            let opts = bincode::options().with_fixint_encoding();
            let mut buf = opts.serialize(&path).unwrap_or_default();
            let len = buf.len();
            let ptr = buf.as_mut_ptr();
            std::mem::forget(buf);
            if !out_len.is_null() {
                *out_len = len;
            }
            ptr
        },
        None => std::ptr::null_mut(),
    }
}
