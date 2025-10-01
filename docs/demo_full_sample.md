# Demo Full Preset Sample Metrics

Running `python demo.py --preset full` with the bundled dataset produces the
following aggregate results (from `aggregate_summary.json`):

- **global ROI:** 2.3452
- **global Sharpe:** 1.2706
- **per-token ROI**
  - ARB (momentum): 1.3077
  - MOM (buy_hold): 5.0000
  - REV (mean_reversion): 0.7280

Use these numbers to verify that your environment matches the reference output
by running the demo and comparing the generated `aggregate_summary.json`.
