# Data files

## investor_demo_prices.json

The `investor_demo_prices.json` dataset backs the investor demo examples. To
keep the demo lightweight, tests verify that `investor_demo.load_prices()`
returns no more than 2000 price points and dates. If you update this data,
please keep the entry count under this limit.

## investor_demo_prices_short.json

`investor_demo_prices_short.json` provides a small subset of the investor demo
price data for quick examples and tests. It mirrors the structure of
`investor_demo_prices.json` but contains fewer records so demos run faster.

## investor_demo_prices_multi.json

`investor_demo_prices_multi.json` contains price histories for multiple tokens
and enables multi-token backtest demonstrations. Use the ``--preset multi``
flag with ``investor_demo.py`` to load this dataset.
