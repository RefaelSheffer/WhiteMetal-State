// src/lib/rules/volatility.js

function rollingStd(values, window) {
  const out = [];
  for (let i = 0; i < values.length; i++) {
    if (i + 1 < window) {
      out.push(null);
      continue;
    }
    const slice = values.slice(i + 1 - window, i + 1).filter((v) => typeof v === "number");
    if (!slice.length) {
      out.push(null);
      continue;
    }
    const mean = slice.reduce((a, b) => a + b, 0) / slice.length;
    const variance = slice.reduce((acc, v) => acc + (v - mean) ** 2, 0) / slice.length;
    out.push(Math.sqrt(variance));
  }
  return out;
}

function computeDailyReturnsFromCloses(series) {
  if (!Array.isArray(series)) return [];
  const returns = [null];
  for (let i = 1; i < series.length; i++) {
    const prev = series[i - 1];
    const curr = series[i];
    if (!prev || !curr) {
      returns.push(null);
      continue;
    }
    const prevClose = typeof prev === "number" ? prev : prev.close;
    const currClose = typeof curr === "number" ? curr : curr.close;
    if (typeof prevClose !== "number" || typeof currClose !== "number") {
      returns.push(null);
      continue;
    }
    returns.push(prevClose !== 0 ? currClose / prevClose - 1 : null);
  }
  return returns;
}

export function computeVolatility(series, window = 20) {
  if (!Array.isArray(series)) return [];
  const closes = series.map((d) => (typeof d === "number" ? d : d?.close));
  const dailyReturns = computeDailyReturnsFromCloses(closes);
  return rollingStd(
    dailyReturns.map((v) => (typeof v === "number" ? v : null)),
    window,
  );
}

export function attachVolatility(series, window = 20) {
  if (!Array.isArray(series)) return [];
  const vol = computeVolatility(series, window);
  return series.map((row, idx) => ({ ...row, vol20: row?.vol20 ?? vol[idx] ?? null }));
}

export function percentile(values, p = 0.7) {
  const clean = values.filter((v) => typeof v === "number" && Number.isFinite(v));
  if (!clean.length) return null;
  const sorted = clean.sort((a, b) => a - b);
  const rank = Math.min(sorted.length - 1, Math.max(0, Math.floor(p * sorted.length)));
  return sorted[rank];
}
