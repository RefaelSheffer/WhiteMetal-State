# Analog/kNN Probability Engine — PRD + Tech Spec (GitHub Pages Ready)

## 0. Objective
Build a fully client-side “probability estimator” that surfaces:
- **P(success)** for a selected horizon (e.g., 5/20/60 days) where success = forward return above a threshold.
- Distribution of outcomes from most similar historical states (percentiles, median, mean, hit rate).
- Confidence indicator derived from neighbor count and similarity quality.
- Transparent explanation: “Based on X similar historical states.”
- Always-on disclaimer: “Not investment advice.”

Runs as a static site (GitHub Pages) with no server dependencies.

## 1. Scope
**In scope**
- Load historical dataset (CSV/JSON) of indicators + prices/returns.
- Optional feature calculation (momentum/volatility/RSI/z-score, etc.) or load precomputed features.
- Similarity search layer (kNN/analog search) with weighted aggregation.
- Outputs: P(success), median/mean, P10/P25/P75/P90, N + quality score, top 10 similar cases.
- Lightweight walk-forward backtest to display quality metrics.
- Minimal, fast UI.

**Out of scope (phase 1)**
- Random Forest/NN/ONNX.
- Heavy optimization (WebGPU).
- Live data API (placeholder file upload is fine).

## 2. User Stories
- As a user, I choose an asset/ticker and horizon (e.g., 20D) and click **Analyze**.
- I see probability + distribution with N and confidence.
- I can view the 10 closest historical analogs (date, feature values, future return).
- I can tweak K/radius/weighting and observe immediate changes.
- I can run **Backtest** to view core quality metrics (AUC/Brier/HitRate or simplified set).

## 3. UX/UI (single screen)
**Controls** (top or right panel)
- Asset dropdown.
- Horizon: 5 / 20 / 60 dropdown.
- Target:
  - “Return > 0”.
  - “Return > X%” (X slider).
- Similarity: K slider (50–500), Weighting (inverse_distance/softmax), Feature set preset (core10/core15), Confidence threshold (min N optional).
- Buttons: Analyze / Backtest.

**Outputs**
- **Card 1: Probability** — P(success) prominent; Confidence label (Low/Med/High) + N.
- **Card 2: Distribution** — mini histogram/sparkline; P10/P25/Median/P75/P90.
- **Card 3: Similar cases** — table of top 10: Date, distance, future_return.
- **Footer** — fixed disclaimer + “Method info” tooltip.

## 4. Disclaimer (must show on all screens)
“המידע מוצג למטרות מידע בלבד ואינו ייעוץ השקעות/שיווק השקעות/תיווך. מודל סטטיסטי על נתוני עבר אינו מבטיח תוצאות עתידיות. השתמש/י בשיקול דעת.”

## 5. Data Contract
Preferred JSON (fast on Pages). Example record:
```json
{
  "t": "2024-06-18",
  "asset": "SLV",
  "close": 26.12,
  "f": {
    "ret_5": 0.012,
    "ret_20": -0.034,
    "ret_60": 0.081,
    "vol_20": 0.019,
    "rsi_14": 52.3,
    "z_ma20": 0.6,
    "z_ma200": -0.2,
    "dd_60": -0.08,
    "atr_14": 0.33,
    "trend_200": 1
  },
  "y": {
    "fwd_5": 0.004,
    "fwd_20": 0.021,
    "fwd_60": -0.017
  }
}
```
- `f`: same-day features.
- `y`: precomputed forward returns (avoid leakage).
- `asset` supports multi-asset datasets.

**If CSV is required**: columns `date,asset,close,ret_5,ret_20,...,fwd_5,fwd_20,fwd_60`.

## 6. Core Algorithm (Analog / Weighted kNN)
### 6.1 Preprocessing
- Filter to `asset == selectedAsset`.
- Drop rows with missing features/targets for the chosen horizon.
- Train/predict split: query is latest (or chosen) row; for backtest use walk-forward.

### 6.2 Feature normalization (required)
- Standardize: `z = (x - mean_train) / std_train` using training-only stats.

### 6.3 Distance metric
- Euclidean on z-scored vector: `d = sqrt(sum((qi - xi)^2))`.
- Optional feature weights: `d = sqrt(sum(w_i * (qi - xi)^2))`.

### 6.4 Neighbor selection
- Compute distance to all train samples; take K smallest; store `(date, d, y_fwd_horizon)`.

### 6.5 Weights
- Inverse distance: `w = 1 / (d + eps)` → normalize.
- Softmax: `w = exp(-d / tau)` → normalize.
- Defaults: `eps = 1e-6`; `tau` default ≈ median neighbor distance.

### 6.6 Target definition
- Success if `y_fwd_horizon > threshold` (threshold default 0).

### 6.7 Probability estimate
- `P = sum(w_i * 1[success_i])`.

### 6.8 Distribution stats
- Weighted mean; weighted median; weighted percentiles (10/25/75/90); hit rate.
- Weighted quantile: sort by value, cumulative weights, choose first index ≥ q. If weights sum to 0, fallback to uniform. Exclude NaN distances.

### 6.9 Confidence score
- `N = K (after filters)`; `effectiveN = 1 / sum(w_i^2)`; `avgDistance = sum(w_i * d_i)`.
- Confidence map: High if effectiveN > 80 and avgDistance < 1.0; Medium if one criterion passes; Low otherwise (tune as needed).

## 7. Backtest (walk-forward)
- Test window: e.g., last 2 years.
- Sample weekly for speed (~100–200 points for 2 years).
- For each t: Train = history before t; Query = features at t; compute P(t); Outcome = success by `y_fwd_horizon`.
- Metrics: Brier Score (primary), optional calibration bins/hit rate thresholds/avg return of top-prob days.
- Performance targets: weekly backtest over 2 years < 3 seconds in browser.

## 8. Project Structure (GitHub Pages)
Recommended Vite + React + TypeScript (or vanilla TS for minimalism).
```
/src
  /data
    dataset_manifest.json
  /lib
    dataLoader.ts
    normalize.ts
    knn.ts
    weightedStats.ts
    backtest.ts
    types.ts
  /ui
    Controls.tsx
    Results.tsx
    SimilarCases.tsx
    BacktestPanel.tsx
  App.tsx
  main.tsx
/public
  data/
    slv_features.json
```

## 9. Types & Interfaces
```ts
export type Horizon = 5 | 20 | 60;

export interface Row {
  t: string;              // ISO date
  asset: string;
  close?: number;
  f: Record<string, number>;
  y: Record<string, number>; // keys: "fwd_5", "fwd_20", ...
}

export interface Normalizer {
  mean: Record<string, number>;
  std: Record<string, number>;
  features: string[];
}

export interface KNNConfig {
  k: number;
  horizon: Horizon;
  threshold: number; // e.g., 0 for >0
  weighting: "inverse_distance" | "softmax";
  tau?: number;      // for softmax
  featureWeights?: Record<string, number>;
}

export interface Neighbor {
  t: string;
  d: number;
  y: number; // future return for horizon
}

export interface AnalysisResult {
  pSuccess: number;
  effectiveN: number;
  n: number;
  avgDistance: number;
  stats: {
    mean: number;
    median: number;
    p10: number;
    p25: number;
    p75: number;
    p90: number;
  };
  neighborsTop: Neighbor[];
  confidenceLabel: "Low" | "Medium" | "High";
}
```

## 10. Controls (defaults)
- Horizon: 20
- Target threshold: 0
- K: 200
- Weighting: inverse_distance
- Feature preset: core10
- Min effectiveN warning threshold: 30

## 11. Feature Presets
- **core10**: ret_5, ret_20, ret_60, vol_20, rsi_14, z_ma20, z_ma200, dd_60, atr_14, trend_200.
- **core15** (optional): core10 + macd_hist, vol_60, ret_1, skew_20, range_20.

## 12. Anti-Leakage Rules
- `y.fwd_*` must use future prices only (no feature leakage).
- Normalization stats (mean/std) derived only from training data at each backtest step.
- Analyze on chosen date: train uses only history before that date (include the query row if predicting forward from today).

## 13. Acceptance Criteria
- Selecting SLV + horizon 20 yields P(success) < 200 ms on ≤50k rows.
- Display P(success), median, P10/P90, N, effectiveN, avgDistance.
- Show top 10 neighbors with date and future return.
- Changing K/weighting updates results immediately.
- Backtest (weekly, 2 years) runs < 3 seconds.
- Disclaimer always visible.
- If effectiveN is low → UI shows “Low confidence / not enough similar history”.

## 14. CODEX Tasks (ready-to-run prompt)
- Create Vite React TS project ready for GitHub Pages deploy.
- Implement `dataLoader.ts` to load `/public/data/<asset>.json` into `Row[]`.
- Implement:
  - `buildNormalizer(rows, featureList)` (mean/std)
  - `applyNormalizer(row, normalizer)` returning vector
- Implement `knnAnalyze(trainRows, queryRow, normalizer, config) -> AnalysisResult`:
  - compute distances
  - select K
  - compute weights
  - compute pSuccess + weighted stats + confidence label
- Implement `backtest.ts` with weekly walk-forward:
  - returns array of `{t, p, outcome}`
  - compute Brier score
- Build UI components: Controls (asset/horizon/K/threshold/weighting), Results cards, Similar cases table, Backtest panel + small chart (optional).
- Add Disclaimer component pinned at bottom.
- Add GH Pages deploy instructions (base path, Vite config).

## 15. Deploy Notes (GitHub Pages)
- Vite config: `base: "/<repo-name>/"`.
- Data served from `/public/data/`.
- Build + deploy via GitHub Actions (static). Provide placeholder for manual upload of fresh data if needed.
