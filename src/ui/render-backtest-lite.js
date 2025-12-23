// src/ui/render-backtest-lite.js
import { fetchJsonWithFallbacks } from "../lib/fetchJson.js";

const HORIZON_OPTIONS = [5, 10, 20, 60];
const MIN_SAMPLE_SIZE = 30;

const state = {
  pricesPromise: null,
};

function fmtPct(value, decimals = 1) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return `${(Number(value) * 100).toFixed(decimals)}%`;
}

function fmtNum(value, decimals = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return Number(value).toFixed(decimals);
}

async function loadPrices() {
  if (!state.pricesPromise) {
    state.pricesPromise = fetchJsonWithFallbacks("raw/slv_daily.json", {
      debugLabel: "slv_daily.json",
      cache: "force-cache",
    })
      .then((res) => normalizePrices(res.data))
      .catch((err) => {
        state.pricesPromise = null;
        throw err;
      });
  }
  return state.pricesPromise;
}

function normalizePrices(data) {
  if (!Array.isArray(data)) throw new Error("Invalid prices payload");
  const cleaned = data
    .filter((row) => row && row.date && typeof row.close === "number" && !Number.isNaN(row.close))
    .sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
  return cleaned.map((row) => ({ date: row.date, close: Number(row.close) }));
}

function forwardReturns(prices, horizon) {
  const out = [];
  for (let i = 0; i + horizon < prices.length; i += 1) {
    const start = prices[i]?.close;
    const end = prices[i + horizon]?.close;
    if (start > 0 && end > 0) {
      out.push(end / start - 1);
    }
  }
  return out;
}

function percentile(sortedValues, p) {
  if (!sortedValues.length) return null;
  const idx = (sortedValues.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sortedValues[lo];
  const weight = idx - lo;
  return sortedValues[lo] * (1 - weight) + sortedValues[hi] * weight;
}

function computeStats(values) {
  const n = values.length;
  if (!n) return { n: 0 };

  const winRate = values.filter((v) => v > 0).length / n;
  const avg = values.reduce((acc, v) => acc + v, 0) / n;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(n / 2);
  const median = n % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
  const variance = values.reduce((acc, v) => acc + (v - avg) ** 2, 0) / n;
  const std = Math.sqrt(variance);

  return {
    n,
    winRate,
    avg,
    median,
    std,
    p10: percentile(sorted, 0.1),
    p90: percentile(sorted, 0.9),
  };
}

function renderTable(container, stats, horizon) {
  const data = stats[horizon];
  if (!data) {
    container.innerHTML = `<div class="muted">Data unavailable.</div>`;
    return;
  }

  if (data.n < MIN_SAMPLE_SIZE) {
    container.innerHTML = `<div class="muted">Not enough history (n&lt;${MIN_SAMPLE_SIZE}).</div>`;
    return;
  }

  container.innerHTML = `
    <table class="table compact">
      <tbody>
        <tr><td class="muted">Win rate</td><td class="mono">${fmtPct(data.winRate)}</td></tr>
        <tr><td class="muted">Avg return</td><td class="mono">${fmtPct(data.avg, 2)}</td></tr>
        <tr><td class="muted">Median</td><td class="mono">${fmtPct(data.median, 2)}</td></tr>
        <tr><td class="muted">Std dev</td><td class="mono">${fmtPct(data.std, 2)}</td></tr>
        <tr><td class="muted">Samples</td><td class="mono">${fmtNum(data.n, 0)}</td></tr>
      </tbody>
    </table>
  `;
}

export async function renderBacktestLite(containerEl, {
  actionToday,
  horizonDaysDefault = 20,
  mode = "baseline",
} = {}) {
  if (!containerEl) return;
  containerEl.innerHTML = `<div class="muted">Loading…</div>`;

  let prices;
  try {
    prices = await loadPrices();
  } catch (err) {
    console.error("backtest-lite failed to load prices", err);
    containerEl.innerHTML = `<div class="muted">Data unavailable.</div>`;
    return;
  }

  const horizons = HORIZON_OPTIONS;
  const stats = horizons.reduce((acc, h) => {
    acc[h] = computeStats(forwardReturns(prices, h));
    return acc;
  }, {});

  const preferred = horizons.includes(horizonDaysDefault) ? horizonDaysDefault : horizons[0];
  const selectId = `backtestHorizon-${Math.random().toString(36).slice(2, 7)}`;

  containerEl.innerHTML = `
    <div class="backtest-lite">
      <div class="flex" style="align-items:center; gap:8px; margin-bottom:6px;">
        <div class="muted small">Horizon</div>
        <select id="${selectId}" class="mono" style="background:#0f172a; color:#e2e8f0; border:1px solid #1f2937; padding:4px 8px; border-radius:6px;">
          ${horizons.map((h) => `<option value="${h}" ${h === preferred ? "selected" : ""}>${h} days</option>`).join("")}
        </select>
        <span class="chip muted" title="Mode">${mode === "action" ? "Action" : "Baseline"}</span>
        ${actionToday ? `<span class="chip muted">Today: ${actionToday}</span>` : ""}
      </div>
      <div id="${selectId}-table"></div>
    </div>
  `;

  const selectEl = containerEl.querySelector(`#${selectId}`);
  const tableEl = containerEl.querySelector(`#${selectId}-table`);
  if (!selectEl || !tableEl) return;

  const update = () => {
    const h = Number(selectEl.value);
    renderTable(tableEl, stats, horizons.includes(h) ? h : preferred);
  };

  selectEl.addEventListener("change", update);
  update();
}
