# Solhunter Observability Bundle

The observability bundle captures the dashboards and alerting rules required to monitor
critical runtime flows such as the event bus, suggestion lifecycle, RL warmup paths,
and paper trading health. The bundle is stored in version control so changes can be reviewed
and reproduced across environments.

## Contents

| Component | Path | Description |
|-----------|------|-------------|
| Grafana dashboard | `dashboards/solhunter-observability.json` | Visualises latency, suggestion throughput, vote windows, paper unrealized PnL, RL shadow fills, and upstream provider failures. |
| Prometheus alert rules | `alerts.yaml` | Alerts tied to the dashboard panels to catch regressions before they reach production. |

## Importing the dashboard

1. Navigate to **Dashboards → Import** in Grafana.
2. Upload `dashboards/solhunter-observability.json` or paste the JSON definition into the import modal.
3. Select the Prometheus data source that backs Solhunter metrics.
4. Save the dashboard as `Solhunter Observability Overview` (the default title).

You can also automate this with the Grafana HTTP API:

```bash
curl -X POST "https://<grafana-host>/api/dashboards/db" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  --data-binary @docs/observability/dashboards/solhunter-observability.json
```

## Deploying the alerts

The alert rules are compatible with the Prometheus/Alertmanager stack and can be deployed with `promtool`
or `kubectl` depending on your environment.

```bash
# Validate syntax before applying
promtool check rules docs/observability/alerts.yaml

# Apply to a Kubernetes cluster that exposes Prometheus rules via CRDs
kubectl apply -f docs/observability/alerts.yaml -n observability
```

Make sure Alertmanager routes the labels defined in `alerts.yaml` so the `dashboard`
label links responders back to the Grafana panels that triggered the incident.

## Price blend diagnostics

The pricing pipeline now emits a dedicated `metrics.prices.blend` stream that carries
per-token quote diagnostics. Each payload includes the most recent providers that
contributed to the blended price, their quote ages, per-venue deviation in basis points,
computed z-scores, and any alerts that tripped (for example `stale_quote`,
`missing_providers`, `sigma_outlier`, or the new `asymmetric_consensus`). The aggregate
object also reports a `skew_bps` value that measures how unbalanced the quote cloud is
around the resolved price so responders can tell the difference between symmetric noise
and a one-sided blowout. Operators can plot these fields to spot when a venue stops
returning data or when a single venue drifts far outside the consensus band.

The UI renders the same diagnostics next to the price widgets so responders can
correlate noisy quotes with shifts in acceptance rate or vote timing. The
`PRICE_BLEND_*` environment variables described in
`tests/golden_pipeline/test_price_blend_chaos.py` let you tighten or relax the alert
thresholds without redeploying code, including `PRICE_BLEND_SKEW_ALERT_BPS` for the
skew detector and the existing spread/sigma knobs.

## CI artifact

The GitHub Actions workflow publishes the contents of `docs/observability/` as the
`observability-bundle` artifact on every run. The artifact can be downloaded from
run summaries and consumed by automated deployment jobs.

## Local iteration

When iterating on dashboards or alerts, keep the JSON and YAML files in sync with the
Grafana instance where changes were tested. The `tests/test_log_scrubber.py` test guards
against accidentally committing secrets when attaching log or UI payload samples used
to validate the dashboards.
