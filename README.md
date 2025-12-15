# WhiteMetal State

Event-driven silver market regime analysis surfaced as a static GitHub Pages site.

## What this repo does
- Generates synthetic SLV price data (no API keys required) inside GitHub Actions.
- Detects market events (shakeout, distribution risk, reclaim, range accumulation).
- Emits a single actionable signal plus lightweight backtest/performance JSON.
- Serves a minimal dashboard that reads the JSON directly from `/public/data`.

Walk-forward validation is baked into the pipeline: every run writes
`public/data/perf/walkforward.json` with a rolling 120-day train / 30-day test
split to keep the signal honest and highlight out-of-sample drift.

## Running locally
```bash
python main.py  # writes public/data/* JSON payloads
python -m http.server 8000  # optional: serve index.html for quick preview
```
Then open http://localhost:8000 to view the dashboard.

To pull the latest live SLV prices, CFTC COT data, and EDGAR filings (with historical COT coverage back to 2008) plus JSON backups, run:
```bash
python scripts/update_data.py  # writes data/*.json and data/backups/<timestamp>/*
```
Use `--loop-daily` to keep the collector running once per day:
```bash
python scripts/update_data.py --loop-daily --interval-hours 24
```
The default start year for COT data is 2008; override with `--start-year` if you need a narrower slice.

## GitHub Actions (suggested)
A workflow can call `python main.py` on a schedule and commit the refreshed JSON
files back to the repository (or the `gh-pages` branch). No secrets are needed
for the current synthetic data path, and the structure is ready for real API
fetchers when credentials are available.

### Cadence and decomposition
- The STL decomposition stored in `public/data/perf/decomposition.json` helps
  validate the dominant cycle length (period) baked into the backtest. If you
  adjust the `period` parameter in `engine/main.py` or downstream components,
  rerun `python main.py` to regenerate the decomposition and confirm the
  frequency assumptions.

### Toward real-time detection
- GitHub Actions can schedule `python main.py` roughly once per day. For
  near-real-time event detection, run the script on a persistent host (e.g.,
  hourly) and publish the refreshed JSON artifacts to `public/data/` so the
  static dashboard always reflects the newest signal and decomposition.

## Directory layout
- `engine/` – data fetchers, event/decision logic, performance calculation.
- `public/data/` – published artifacts read by the static site.
- `index.html` – dashboard that visualizes the latest signal and performance.
