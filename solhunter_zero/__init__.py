"""Top level package for :mod:`solhunter_zero`.

This module originally imported several submodules on import which pulled in
heavy optional dependencies like ``solders``. Many unit tests only need to
access lightweight utilities such as :mod:`backtester` and therefore should
not fail simply because those optional packages are missing. To avoid that we
lazily import the heavy modules when the relevant attributes are accessed.
"""

__version__ = "0.1.0"

__all__ = [
    "load_keypair",
    "fetch_token_prices",
    "fetch_token_prices_async",
    "fetch_price_quotes_async",
    "RiskManager",
    "covariance_matrix",
    "correlation_matrix",
    "average_correlation",
    "portfolio_cvar",
    "portfolio_evar",
    "portfolio_variance",
    "entropic_value_at_risk",
    "rolling_backtest",
    "bayesian_optimize_parameters",
    "PopulationRL",
    "MultiAgentRL",
    "RLTraining",
    "RayTraining",
    "RLDaemon",
    "RLWeightAgent",
]


def __getattr__(name: str):
    if name == "load_keypair":
        from .wallet import load_keypair as func

        return func
    if name == "fetch_token_prices":
        from .prices import fetch_token_prices as func

        return func
    if name == "fetch_token_prices_async":
        from .prices import fetch_token_prices_async as func

        return func
    if name == "fetch_price_quotes_async":
        from .prices import fetch_price_quotes_async as func

        return func
    if name == "RiskManager":
        from .risk import RiskManager as cls

        return cls
    if name == "covariance_matrix":
        from .risk import covariance_matrix as func

        return func
    if name == "portfolio_cvar":
        from .risk import portfolio_cvar as func

        return func
    if name == "portfolio_evar":
        from .risk import portfolio_evar as func

        return func
    if name == "portfolio_variance":
        from .risk import portfolio_variance as func

        return func
    if name == "correlation_matrix":
        from .risk import correlation_matrix as func

        return func
    if name == "average_correlation":
        from .risk import average_correlation as func

        return func
    if name == "entropic_value_at_risk":
        from .risk import entropic_value_at_risk as func

        return func
    if name == "rolling_backtest":
        from .backtest_pipeline import rolling_backtest as func

        return func
    if name == "bayesian_optimize_parameters":
        from .backtest_pipeline import bayesian_optimize_parameters as func

        return func
    if name == "PopulationRL":
        from .multi_rl import PopulationRL as cls

        return cls
    if name == "MultiAgentRL":
        from .rl_training import MultiAgentRL as cls

        return cls
    if name == "RLTraining":
        from .rl_training import RLTraining as cls

        return cls
    if name == "RayTraining":
        from .ray_training import RayTraining as cls

        return cls
    if name == "RLDaemon":
        from .rl_daemon import RLDaemon as cls

        return cls
    if name == "RLWeightAgent":
        from .agents.rl_weight_agent import RLWeightAgent as cls

        return cls
    raise AttributeError(name)
