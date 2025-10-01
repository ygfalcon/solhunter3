use std::collections::{HashMap, HashSet, BinaryHeap};
use serde::{Deserialize, Serialize};

use crate::TokenInfo;

#[derive(Debug)]
pub struct RouteResult {
    pub path: Vec<String>,
    pub profit: f64,
    pub slippage: f64,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct AdjacencyMatrix {
    pub venues: Vec<String>,
    pub matrix: Vec<f64>,
}

pub fn build_adjacency_matrix(
    dex_map: &HashMap<String, HashMap<String, TokenInfo>>,
    token: &str,
) -> Option<AdjacencyMatrix> {
    let mut prices: HashMap<String, f64> = HashMap::new();
    let mut bids: HashMap<String, f64> = HashMap::new();
    let mut asks: HashMap<String, f64> = HashMap::new();
    for (dex, tokens) in dex_map.iter() {
        if let Some(info) = tokens.get(token) {
            let price = if info.asks > 0.0 && info.bids > 0.0 {
                (info.asks + info.bids) / 2.0
            } else if info.asks > 0.0 {
                info.asks
            } else {
                info.bids
            };
            prices.insert(dex.clone(), price);
            bids.insert(dex.clone(), info.bids);
            asks.insert(dex.clone(), info.asks);
        }
    }
    if prices.len() < 2 {
        return None;
    }
    let venues: Vec<String> = prices.keys().cloned().collect();
    let n = venues.len();
    let mut matrix = vec![f64::NEG_INFINITY; n * n];
    for (i, a) in venues.iter().enumerate() {
        for (j, b) in venues.iter().enumerate() {
            if i == j {
                continue;
            }
            let pa = prices.get(a).copied().unwrap_or(0.0);
            let pb = prices.get(b).copied().unwrap_or(0.0);
            let ask_liq = asks.get(a).copied().unwrap_or(0.0).max(1.0);
            let bid_liq = bids.get(b).copied().unwrap_or(0.0).max(1.0);
            let coeff = (pb - pa) - pa * (1.0 / ask_liq + 1.0 / bid_liq);
            matrix[i * n + j] = coeff;
        }
    }
    Some(AdjacencyMatrix { venues, matrix })
}

#[derive(Clone, PartialEq)]
struct IdxNode {
    neg_profit: f64,
    path: Vec<usize>,
    visited: HashSet<usize>,
}

impl Eq for IdxNode {}

impl Ord for IdxNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .neg_profit
            .partial_cmp(&self.neg_profit)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl PartialOrd for IdxNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub fn best_route_matrix(
    adj: &AdjacencyMatrix,
    amount: f64,
    max_hops: usize,
) -> Option<RouteResult> {
    let n = adj.venues.len();
    if n < 2 || max_hops < 2 {
        return None;
    }
    let mut best_path: Vec<usize> = Vec::new();
    let mut best_profit = f64::NEG_INFINITY;
    let mut heap: BinaryHeap<IdxNode> = BinaryHeap::new();
    for i in 0..n {
        let mut visited = HashSet::new();
        visited.insert(i);
        heap.push(IdxNode {
            neg_profit: 0.0,
            path: vec![i],
            visited,
        });
    }
    while let Some(IdxNode { neg_profit, path, visited }) = heap.pop() {
        let profit = -neg_profit;
        if path.len() > 1 && profit > best_profit {
            best_profit = profit;
            best_path = path.clone();
        }
        if path.len() >= max_hops {
            continue;
        }
        let last = *path.last().unwrap();
        for j in 0..n {
            if visited.contains(&j) || j == last {
                continue;
            }
            let coeff = adj.matrix[last * n + j];
            if coeff.is_finite() {
                let val = coeff * amount;
                let mut new_path = path.clone();
                new_path.push(j);
                let mut new_vis = visited.clone();
                new_vis.insert(j);
                let new_profit = profit + val;
                heap.push(IdxNode {
                    neg_profit: -new_profit,
                    path: new_path,
                    visited: new_vis,
                });
            }
        }
    }
    if best_profit.is_finite() {
        let path = best_path
            .into_iter()
            .map(|i| adj.venues[i].clone())
            .collect();
        Some(RouteResult { path, profit: best_profit, slippage: 0.0 })
    } else {
        None
    }
}

pub fn best_route(
    dex_map: &HashMap<String, HashMap<String, TokenInfo>>,
    token: &str,
    amount: f64,
    max_hops: usize,
) -> Option<RouteResult> {
    if max_hops < 2 {
        return None;
    }
    let mut prices: HashMap<String, f64> = HashMap::new();
    let mut bids: HashMap<String, f64> = HashMap::new();
    let mut asks: HashMap<String, f64> = HashMap::new();
    for (dex, tokens) in dex_map.iter() {
        if let Some(info) = tokens.get(token) {
            let price = if info.asks > 0.0 && info.bids > 0.0 {
                (info.asks + info.bids) / 2.0
            } else if info.asks > 0.0 {
                info.asks
            } else {
                info.bids
            };
            prices.insert(dex.clone(), price);
            bids.insert(dex.clone(), info.bids);
            asks.insert(dex.clone(), info.asks);
        }
    }
    if prices.len() < 2 {
        return None;
    }
    let venues: Vec<String> = prices.keys().cloned().collect();
    let mut best_path: Option<Vec<String>> = None;
    let mut best_profit = f64::NEG_INFINITY;
    let mut best_slip = 0.0;
    let max_price = prices.values().fold(f64::MIN, |a, v| a.max(*v));
    let min_price = prices.values().fold(f64::MAX, |a, v| a.min(*v));
    let max_diff = (max_price - min_price).abs();
    let mut paths: Vec<(String, Vec<String>, f64, f64)> = venues
        .iter()
        .map(|v| (v.clone(), vec![v.clone()], 0.0, 0.0))
        .collect();
    for _ in 1..max_hops {
        let mut new_paths = Vec::new();
        for (last, path, profit, slip) in paths.iter() {
            let remaining = max_hops.saturating_sub(path.len());
            if *profit + (remaining as f64) * max_diff * amount < best_profit {
                continue;
            }
            for nxt in venues.iter() {
                if path.contains(nxt) {
                    continue;
                }
                let buy_price = prices.get(last).copied().unwrap_or(0.0);
                let sell_price = prices.get(nxt).copied().unwrap_or(0.0);
                let p = (sell_price - buy_price) * amount;
                let ask_liq = asks.get(last).copied().unwrap_or(0.0).max(1.0);
                let bid_liq = bids.get(nxt).copied().unwrap_or(0.0).max(1.0);
                let slip_val = amount / ask_liq + amount / bid_liq;
                let new_profit = profit + p - slip_val * buy_price;
                let new_slip = slip + slip_val;
                let mut new_path = path.clone();
                new_path.push(nxt.clone());
                if new_profit > best_profit {
                    best_profit = new_profit;
                    best_path = Some(new_path.clone());
                    best_slip = new_slip;
                }
                if new_profit
                    + (max_hops.saturating_sub(new_path.len()) as f64) * max_diff * amount
                    >= best_profit
                {
                    new_paths.push((nxt.clone(), new_path, new_profit, new_slip));
                }
            }
        }
        paths = new_paths;
        if paths.is_empty() {
            break;
        }
    }
    best_path.map(|p| RouteResult { path: p, profit: best_profit, slippage: best_slip })
}
