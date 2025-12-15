"""Pipeline entrypoint for GitHub Actions.

This script downloads real SLV OHLCV data (or reuses the cached JSON on
subsequent runs), detects events, produces a signal, and emits
lightweight backtest metrics to `public/data`. It is intentionally
dependency-light so it can run on GitHub's hosted runners without extra
setup.
"""
from engine.main import run_pipeline


if __name__ == "__main__":
    run_pipeline()
