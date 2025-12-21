const DEFAULT_FEATURES = ["ret_5", "ret_20", "vol_20", "rsi_14", "z_ma20"];

function rollingMean(values, window) {
  const out = [];
  for (let i = 0; i < values.length; i++) {
    if (i + 1 < window) {
      out.push(null);
      continue;
    }
    const slice = values.slice(i + 1 - window, i + 1);
    const sum = slice.reduce((a, b) => a + b, 0);
    out.push(sum / window);
  }
  return out;
}

function rollingStd(values, window) {
  const out = [];
  const means = rollingMean(values, window);
  for (let i = 0; i < values.length; i++) {
    if (i + 1 < window) {
      out.push(null);
      continue;
    }
    const slice = values.slice(i + 1 - window, i + 1);
    const mean = means[i] ?? 0;
    const variance = slice.reduce((acc, v) => acc + (v - mean) ** 2, 0) / window;
    out.push(Math.sqrt(variance));
  }
  return out;
}

function computeRsi(values, period = 14) {
  const gains = [null];
  const losses = [null];
  for (let i = 1; i < values.length; i++) {
    const change = values[i] - values[i - 1];
    gains.push(Math.max(change, 0));
    losses.push(Math.max(-change, 0));
  }

  const rsi = [null];
  for (let i = 1; i < values.length; i++) {
    if (i < period) {
      rsi.push(null);
      continue;
    }
    const gainSlice = gains.slice(i - period + 1, i + 1);
    const lossSlice = losses.slice(i - period + 1, i + 1);
    const avgGain = gainSlice.reduce((a, b) => a + b, 0) / period;
    const avgLoss = lossSlice.reduce((a, b) => a + b, 0) / period;
    const rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
    rsi.push(100 - 100 / (1 + rs));
  }
  return rsi;
}

function forwardReturn(closes, idx, horizon) {
  if (idx + horizon >= closes.length) return null;
  const start = closes[idx];
  const end = closes[idx + horizon];
  if (!start || !end) return null;
  return end / start - 1;
}

function pctChange(closes, idx, window) {
  if (idx - window < 0) return null;
  const start = closes[idx - window];
  const end = closes[idx];
  if (!start || !end) return null;
  return end / start - 1;
}

function drawdown(values, idx, window) {
  if (idx - window < 0) return null;
  const slice = values.slice(idx - window, idx + 1).filter((v) => v !== null && v !== undefined);
  if (!slice.length) return null;
  const peak = Math.max(...slice);
  const trough = values[idx];
  if (!peak || trough === null || trough === undefined) return null;
  return trough / peak - 1;
}

export function buildFeatureRows(dates, closes, asset = "SLV") {
  const dailyReturns = [null];
  for (let i = 1; i < closes.length; i++) {
    if (!closes[i] || !closes[i - 1]) {
      dailyReturns.push(null);
      continue;
    }
    dailyReturns.push(closes[i] / closes[i - 1] - 1);
  }

  const vol20 = rollingStd(
    dailyReturns.map((v) => (typeof v === "number" ? v : 0)),
    20
  );
  const ma20 = rollingMean(closes, 20);
  const ma200 = rollingMean(closes, 200);
  const std20 = rollingStd(closes, 20);
  const rsi14 = computeRsi(closes, 14);

  const rows = [];
  for (let i = 0; i < closes.length; i++) {
    const featureRow = {
      t: dates[i],
      asset,
      close: closes[i],
      f: {
        ret_5: pctChange(closes, i, 5),
        ret_20: pctChange(closes, i, 20),
        ret_60: pctChange(closes, i, 60),
        vol_20: vol20[i],
        rsi_14: rsi14[i],
        z_ma20: std20[i] ? (closes[i] - (ma20[i] ?? 0)) / std20[i] : null,
        trend_200: ma200[i] ? (closes[i] - ma200[i]) / ma200[i] : null,
        dd_60: drawdown(closes, i, 60),
      },
      y: {
        fwd_5: forwardReturn(closes, i, 5),
        fwd_20: forwardReturn(closes, i, 20),
        fwd_60: forwardReturn(closes, i, 60),
      },
    };
    rows.push(featureRow);
  }
  return rows;
}

function validRowForFeatures(row, featureList) {
  if (!row || !row.f) return false;
  return featureList.every((key) => typeof row.f[key] === "number" && Number.isFinite(row.f[key]));
}

export function buildNormalizer(rows, featureList) {
  const mean = {};
  const std = {};
  featureList.forEach((f) => {
    const vals = rows
      .map((r) => r?.f?.[f])
      .filter((v) => typeof v === "number" && Number.isFinite(v));
    if (!vals.length) {
      mean[f] = 0;
      std[f] = 1;
      return;
    }
    const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
    const variance = vals.reduce((acc, v) => acc + (v - avg) ** 2, 0) / vals.length;
    mean[f] = avg;
    std[f] = Math.max(Math.sqrt(variance), 1e-8);
  });
  return { mean, std, features: [...featureList] };
}

export function applyNormalizer(row, normalizer) {
  if (!row || !row.f) return null;
  const vector = [];
  for (const f of normalizer.features) {
    const val = row.f[f];
    if (typeof val !== "number" || !Number.isFinite(val)) return null;
    const mu = normalizer.mean[f] ?? 0;
    const sigma = normalizer.std[f] ?? 1;
    vector.push((val - mu) / sigma);
  }
  return vector;
}

function euclideanDistance(a, b) {
  if (!a || !b || a.length !== b.length) return null;
  let sum = 0;
  for (let i = 0; i < a.length; i++) {
    const diff = a[i] - b[i];
    sum += diff * diff;
  }
  return Math.sqrt(sum / a.length);
}

function weightVector(distances, weighting, tau) {
  const eps = 1e-6;
  if (weighting === "softmax") {
    const scale = tau ?? (distances.length ? median(distances) : 1) || 1;
    const weights = distances.map((d) => Math.exp(-d / scale));
    const total = weights.reduce((a, b) => a + b, 0) || 1;
    return weights.map((w) => w / total);
  }
  const inv = distances.map((d) => 1 / (d + eps));
  const total = inv.reduce((a, b) => a + b, 0) || 1;
  return inv.map((w) => w / total);
}

function weightedQuantile(values, weights, q) {
  if (!values.length || values.length !== weights.length) return null;
  const pairs = values.map((v, i) => ({ v, w: weights[i] }));
  pairs.sort((a, b) => a.v - b.v);
  const total = pairs.reduce((acc, p) => acc + p.w, 0);
  if (!total) return null;
  let acc = 0;
  for (const p of pairs) {
    acc += p.w;
    if (acc / total >= q) return p.v;
  }
  return pairs[pairs.length - 1].v;
}

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) return (sorted[mid - 1] + sorted[mid]) / 2;
  return sorted[mid];
}

function confidenceLabel(effectiveN, avgDistance) {
  if (effectiveN > 80 && avgDistance < 1.0) return "High";
  if (effectiveN > 40 || avgDistance < 1.4) return "Medium";
  return "Low";
}

export function knnAnalogProbability(rows, config = {}) {
  const featureList = config.features?.length ? config.features : DEFAULT_FEATURES;
  const horizon = config.horizon ?? 20;
  const k = config.k ?? 120;
  const threshold = config.threshold ?? 0;
  const weighting = config.weighting ?? "inverse_distance";

  const targetKey = `fwd_${horizon}`;
  const queryIndex = config.queryIndex ?? rows.length - 1;
  const trainRows = rows.filter((_, idx) => idx < queryIndex && validRowForFeatures(rows[idx], featureList));
  const queryRow = rows[queryIndex];

  if (!validRowForFeatures(queryRow, featureList) || !trainRows.length) {
    return null;
  }

  const labeledRows = trainRows.filter((r) => typeof r?.y?.[targetKey] === "number" && Number.isFinite(r.y[targetKey]));
  if (!labeledRows.length) return null;

  const normalizer = buildNormalizer(labeledRows, featureList);
  const queryVector = applyNormalizer(queryRow, normalizer);
  if (!queryVector) return null;

  const candidates = [];
  labeledRows.forEach((row) => {
    const vec = applyNormalizer(row, normalizer);
    if (!vec) return;
    const dist = euclideanDistance(queryVector, vec);
    if (dist === null) return;
    candidates.push({
      t: row.t,
      d: dist,
      y: row.y[targetKey],
    });
  });

  if (!candidates.length) return null;

  candidates.sort((a, b) => a.d - b.d);
  const neighbors = candidates.slice(0, Math.min(k, candidates.length));
  const distances = neighbors.map((n) => n.d);
  const weights = weightVector(distances, weighting, config.tau);

  const successes = neighbors.map((n) => (n.y > threshold ? 1 : 0));
  const totalWeight = weights.reduce((a, b) => a + b, 0) || 1;
  const pSuccess = neighbors.reduce((acc, n, idx) => acc + weights[idx] * successes[idx], 0);
  const weightedMean = neighbors.reduce((acc, n, idx) => acc + weights[idx] * n.y, 0);
  const med = weightedQuantile(neighbors.map((n) => n.y), weights, 0.5);
  const p10 = weightedQuantile(neighbors.map((n) => n.y), weights, 0.1);
  const p90 = weightedQuantile(neighbors.map((n) => n.y), weights, 0.9);
  const p25 = weightedQuantile(neighbors.map((n) => n.y), weights, 0.25);
  const p75 = weightedQuantile(neighbors.map((n) => n.y), weights, 0.75);

  const effN = 1 / weights.reduce((acc, w) => acc + w * w, 0);
  const avgDistance = neighbors.reduce((acc, n, idx) => acc + n.d * (weights[idx] / totalWeight), 0);

  return {
    pSuccess,
    effectiveN: effN,
    n: neighbors.length,
    avgDistance,
    stats: {
      mean: weightedMean,
      median: med,
      p10,
      p25,
      p75,
      p90,
    },
    neighborsTop: neighbors.slice(0, 10),
    confidenceLabel: confidenceLabel(effN, avgDistance),
    description: `Based on ${neighbors.length} nearest days (effective N ${effN.toFixed(0)}). Weighted probability of gain over ${horizon}d is ${(pSuccess * 100).toFixed(1)}%.`,
  };
}
