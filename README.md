# WhiteMetal State

Event-driven silver market regime analysis surfaced as a static GitHub Pages site.

## What this repo does
- Generates synthetic SLV price data (no API keys required) inside GitHub Actions.
- Detects market events (shakeout, distribution risk, reclaim, range accumulation).
- Emits a single actionable signal plus lightweight backtest/performance JSON.
- Serves a minimal dashboard that reads the JSON directly from `/public/data`.

## Running locally
```bash
python main.py  # writes public/data/* JSON payloads
python -m http.server 8000  # optional: serve index.html for quick preview
```
Then open http://localhost:8000 to view the dashboard.

## GitHub Actions (suggested)
A workflow can call `python main.py` on a schedule and commit the refreshed JSON
files back to the repository (or the `gh-pages` branch). No secrets are needed
for the current synthetic data path, and the structure is ready for real API
fetchers when credentials are available.

## Directory layout
- `engine/` – data fetchers, event/decision logic, performance calculation.
- `public/data/` – published artifacts read by the static site.
- `index.html` – dashboard that visualizes the latest signal and performance.
