# Architecture

## Agents

The trading logic is implemented by a swarm of small agents:

- **DiscoveryAgent** — finds new token listings using the existing scanners.
- **SimulationAgent** — runs Monte Carlo simulations per token.
- **ConvictionAgent** — rates tokens based on expected ROI.
- **MetaConvictionAgent** — aggregates multiple conviction signals.
- **ArbitrageAgent** — detects DEX price discrepancies.
-   The agent polls multiple venues simultaneously and chooses
    the most profitable route accounting for per‑DEX fees, gas and
    latency.  Custom costs can be configured with `dex_fees`,
    `dex_gas` and `dex_latency`.  These latency values are
    measured concurrently at startup by pinging each API and
    websocket endpoint.  Set `MEASURE_DEX_LATENCY=0` to skip
    this automatic measurement. Ongoing refreshes are controlled by
    `dex_latency_refresh_interval`.
    When the optional `route_ffi`
    library is available the agent uses it automatically for
    path computations. Set `USE_FFI_ROUTE=0` to disable this
    behavior.
- **ExitAgent** — proposes sells when stop-loss, take-profit or trailing stop thresholds are hit.
- **ExecutionAgent** — rate‑limited order executor.
  When `PRIORITY_FEES` is set the agent scales the compute-unit price
  based on mempool transaction rate so high-priority submits use a
  larger fee.
- **MempoolSniperAgent** — bundles buys when liquidity appears or a mempool
  event exceeds `mempool_threshold` and submits them with `MEVExecutor`.
- **MemoryAgent** — records past trades for analysis. Trade context and emotion
  tags are saved to `memory.db` and a FAISS index (`trade.index`) for semantic
  search.
- **Adaptive memory** — each agent receives feedback on the outcome of its last
  proposal. The swarm logs success metrics to the advanced memory module and
  agents can query this history to refine future simulations.
- **EmotionAgent** — assigns emotion tags such as "confident" or "anxious" after each trade based on conviction delta, regret level and simulation misfires.
- **ReinforcementAgent** — learns from trade history using Q-learning.
- **DQNAgent** — deep Q-network that learns optimal trade actions.
- **PPOAgent** — actor-critic model trained on offline order book history.
- **SACAgent** — soft actor-critic with continuous trade sizing.
- **RamanujanAgent** — proposes deterministic buys or sells from a hashed conviction score.
- **StrangeAttractorAgent** — chaotic Lorenz model seeded with order-book depth,
  mempool entropy and conviction velocity. Trades when divergence aligns with a
  known profitable manifold.
 - **FractalAgent** — matches ROI fractal patterns using wavelet fingerprints.
- **ArtifactMathAgent** — evaluates simple math expressions using `load_artifact_math` to read `solhunter_zero/data/artifact_math.json`.
- **AlienCipherAgent** — logistic-map strategy that reads coefficients via `load_alien_cipher` from `solhunter_zero/data/alien_cipher.json`.
- **PortfolioAgent** — maintains per-token allocation using `max_allocation` and buys small amounts when idle with `buy_risk`.
- **PortfolioOptimizer** — adjusts positions using mean-variance analysis and risk metrics.
- **CrossDEXRebalancer** — distributes trades across venues according to order-book depth, measured latency and per‑venue fees. It asks `PortfolioOptimizer` for base actions,
  splits them between venues with the best liquidity and fastest response, then forwards the resulting
  orders to `ExecutionAgent` (or `MEVExecutor` bundles when enabled) so other
  strategy agents can coordinate around the final execution.
- **CrossDEXArbitrage** — searches for multi-hop swap chains using the `route_ffi`
  path finder. Latency for each venue is measured with `measure_dex_latency_async`
  and combined with `dex_fees`, `dex_gas` and `dex_latency` to select the most
  profitable route. Limit the search depth with `max_hops`.

Agents can be enabled or disabled in the configuration and their impact
controlled via the `agent_weights` table.  When dynamic weighting is enabled,
the `AgentManager` updates these weights automatically over time based on each
agent's trading performance.

The `AgentManager` periodically adjusts these weights using the
`update_weights()` method.  It reviews trade history recorded by the
`MemoryAgent` and slightly increases the weight of agents with a positive ROI
while decreasing the weight of those with losses.
Every ``evolve_interval`` iterations the manager also calls ``evolve()`` to
spawn new agent mutations and prune those with an ROI below
``mutation_threshold``.
If multiple weight presets are provided via ``weight_config_paths`` the manager
evaluates them every ``strategy_rotation_interval`` iterations and activates the
best performing set.
Each trade outcome is also logged to the advanced memory. Agents look up
previous success rates when deciding whether to accept new simulation results.

Emotion tags produced by the `EmotionAgent` are stored alongside each trade.
Reinforcement agents can read these tags to temper their proposals. A streak of
negative emotions like `anxious` or `regret` reduces conviction in later
iterations, while positive emotions encourage larger allocations.
### Custom Agents via Entry Points

Third-party packages can register their own agent classes under the
`solhunter_zero.agents` entry point group. Any entry points found in this
group are merged into the built-in registry and can be loaded just like the
bundled agents.

```toml
[project.entry-points."solhunter_zero.agents"]
myagent = "mypkg.agents:MyAgent"
```

Example custom agent:

```python
# mypkg/agents.py
from solhunter_zero.agents import BaseAgent


class MyAgent(BaseAgent):
    name = "myagent"

    async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None):
        # implement strategy
        return []
```

## Architecture Details

- **Agent execution and roles** — `AgentManager` launches all agents
  concurrently with `asyncio.gather` via `AgentSwarm` and merges their trade
  proposals. Typical agents handle discovery, simulation, conviction scoring,
  arbitrage checks, exits, execution, logging and reinforcement learning.
- **Persistence** — trade history and order book snapshots are stored in
  `offline_data.db` for offline training. PPO models are periodically retrained
  on this dataset and saved to disk for inference. `MemoryAgent` also records
  trades in `memory.db` for ROI tracking.
- **Advanced forecasting** — transformer, LSTM and graph-based models can be trained on the
  collected snapshots using `scripts/train_transformer_model.py`,
  `scripts/train_price_model.py`, `scripts/train_graph_model.py` or the offline
  `scripts/train_transformer_agent.py`. Set `PRICE_MODEL_PATH` or
  `GRAPH_MODEL_PATH` to the resulting model file and `predict_price_movement()`
  will load it automatically. The path may point to a saved ``TransformerModel``,
  ``DeepTransformerModel``, ``XLTransformerModel`` or ``GraphPriceModel``.
- **Early activity detection** — `scripts/train_activity_model.py` builds a
  dataset from `offline_data.db` and trains a classifier on depth change,
  transaction rate, whale activity and average swap size. Set
  `ACTIVITY_MODEL_PATH` to enable ranking with this model.
- **VaR forecasting** — `scripts/train_var_forecaster.py` trains an LSTM on `offline_data.db` prices. Set `VAR_MODEL_PATH` so `RiskManager` can scale risk using the forecast.
- **Attention-based weighting** — `scripts/train_attention_swarm.py` trains a
  small transformer on `memory.db` trades. Set `attention_swarm_model` in the
  config and `use_attention_swarm = true` to enable this model at run time.
- **RL-based weighting** — set `use_rl_weights = true` so `RLWeightAgent`
  combines a reinforcement learning policy with ROI-based weights.
- **Route ranking** — `scripts/train_gat_route_gnn.py` trains a graph neural
  network on past trades. Add `--gat` to enable the attention-based variant.
- **Scheduling loop** — trading iterations run in a time-driven loop using
  `asyncio` with a default delay of 60 s. The optional Flask Web UI runs
  this loop in a dedicated thread while the web server handles requests.
- **Weight updates and ROI** — `SwarmCoordinator` computes ROI for each agent
  using the `MemoryAgent` logs and normalizes these values to produce
  per‑agent confidence scores supplied to `AgentSwarm` during evaluation.
- **Tactical overlay** — the coordinator also builds a rolling performance
  profile per agent that tracks win-rate, realized-notional dispersion and trade
  depth.  These metrics produce a multiplicative "tactical" boost that rewards
  consistent execution and penalises high-volatility or low‑sample strategies,
  ensuring the hive mind leans into agents that deliver reliable alpha.
- **Token discovery fallback** — the default `websocket` discovery mode uses
  BirdEye when `BIRDEYE_API_KEY` is set and continues with mempool, DEX and
  on-chain scanning when the key is missing.
- **Discovery ranking** — tokens from trending APIs, mempool events and on-chain
  scans are combined, deduplicated and sorted by volume and liquidity.
- **Web UI polling** — the browser polls `/positions`, `/trades`, `/roi`,
  `/risk` and `/weights` every 5 s. It assumes a single user and exposes
  JSON endpoints to inspect trades and ROI history.
- **Status endpoint** — `/status` reports if the trading loop, RL daemon,
  depth service and external event bus are alive.
- **Alerts and position sizing** — no Telegram or other alerting is built in.
  `RiskManager.adjusted()` factors whale liquidity share, mempool transaction
  rate and `min_portfolio_value` into position sizing.

## Backtesting and Datasets