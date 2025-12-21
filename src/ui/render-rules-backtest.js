// src/ui/render-rules-backtest.js
import { backtestRuleset, DEFAULT_RULESET } from "../lib/rules/backtest-ruleset.js";
import { attachTooltips } from "./tooltips.js";

const DISCLAIMER = "This is an educational research tool. Rules-based signals and backtests are simplified and may be wrong. Not investment advice.";

function fmtPct(value, decimals = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return `${(Number(value) * 100).toFixed(decimals)}%`;
}

function fmtNum(value, decimals = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return Number(value).toFixed(decimals);
}

function renderKpis(container, result) {
  const k = result?.kpis || {};
  const b = result?.benchmark || {};
  container.innerHTML = `
    <div class="kpis">
      <div class="kpi"><div class="muted">Net Return</div><div class="v mono">${fmtPct(k.totalReturnPct)}</div></div>
      <div class="kpi"><div class="muted">CAGR</div><div class="v mono">${fmtPct(k.cagrPct)}</div></div>
      <div class="kpi"><div class="muted">Max DD</div><div class="v mono">${fmtPct(k.maxDrawdownPct)}</div></div>
      <div class="kpi"><div class="muted">Sharpe</div><div class="v mono">${fmtNum(k.sharpe)}</div></div>
      <div class="kpi"><div class="muted">Win rate</div><div class="v mono">${fmtPct(k.winRatePct)}</div></div>
      <div class="kpi"><div class="muted"># Trades</div><div class="v mono">${k.tradesCount ?? 0}</div></div>
      <div class="kpi"><div class="muted">Exposure</div><div class="v mono">${fmtPct(k.exposurePct)}</div></div>
    </div>
    <div class="muted" style="margin-top:8px;">Buy & Hold: ${fmtPct(b.buyHoldTotalReturnPct)} · MaxDD ${fmtPct(b.buyHoldMaxDrawdownPct)}</div>
  `;
}

function renderTrades(container, trades) {
  if (!Array.isArray(trades) || !trades.length) {
    container.innerHTML = `<tr><td colspan="6" class="muted">No trades</td></tr>`;
    return;
  }
  container.innerHTML = trades
    .slice(-30)
    .reverse()
    .map((t) => `
      <tr>
        <td>${t.entryDate || "—"}</td>
        <td>${t.exitDate || "—"}</td>
        <td>${t.holdingDays ?? 0}d</td>
        <td>${fmtPct(t.netReturnPct, 2)}</td>
        <td>${t.exitReason || "—"}</td>
        <td>${fmtPct(t.sizePct, 0)}</td>
      </tr>
    `)
    .join("");
}

function plotEquity(curve, benchmark) {
  const el = document.getElementById("rulesEquityChart");
  if (!el) return;
  if (!curve?.length) { el.textContent = "No data"; return; }
  const dates = curve.map((r) => r.date);
  const net = curve.map((r) => r.equityNet);
  const gross = curve.map((r) => r.equityGross);
  const buyHold = benchmark?.buyHold || [];

  const traces = [
    { x: dates, y: net, mode: "lines", name: "Strategy (Net)", line: { color: "#4c9cff" } },
    { x: dates, y: gross, mode: "lines", name: "Strategy (Gross)", line: { color: "#9ca3af", dash: "dot" } },
  ];
  if (buyHold.length) {
    traces.push({ x: buyHold.map((p) => p.date), y: buyHold.map((p) => p.equity), mode: "lines", name: "Buy & Hold", line: { color: "#fbbf24", dash: "dash" } });
  }

  Plotly.newPlot(el, traces, { paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", margin: { l: 45, r: 10, t: 12, b: 40 }, xaxis: { color: "#a8b4d6" }, yaxis: { color: "#a8b4d6", gridcolor: "#1e2a45" } }, { displayModeBar: false });
}

function controlsHtml() {
  return `
    <div class="settings grid-3">
      <div>
        <label class="small">Entry P(up) ≥</label>
        <input type="number" min="0" max="1" step="0.01" id="rulesEntry" value="${DEFAULT_RULESET.entryThreshold}" />
      </div>
      <div>
        <label class="small">Exit P(up) &lt;</label>
        <input type="number" min="0" max="1" step="0.01" id="rulesExit" value="${DEFAULT_RULESET.exitThreshold}" />
      </div>
      <div>
        <label class="small">Take profit (%)</label>
        <input type="number" min="0" max="50" step="0.5" id="rulesTp" value="${DEFAULT_RULESET.takeProfitPct * 100}" />
      </div>
      <div>
        <label class="small">Vol threshold (optional)</label>
        <input type="number" step="0.001" id="rulesVol" placeholder="auto 70th pct" />
      </div>
      <div>
        <label class="small">Add cooldown (days)</label>
        <input type="number" min="0" max="60" step="1" id="rulesCooldown" value="${DEFAULT_RULESET.addCooldownDays}" />
      </div>
      <div>
        <label class="small">Fees on?</label>
        <select id="rulesFees">
          <option value="on">Yes (0.10% + 0.02%)</option>
          <option value="off">No</option>
        </select>
      </div>
    </div>
    <div class="controls" style="margin-top:10px;">
      <button class="btn" id="rulesRun">Run Rules Backtest</button>
      <span class="muted small">Execution: next close · Long-only · No lookahead</span>
    </div>
  `;
}

function readRulesetFromUi() {
  const entry = Number(document.getElementById("rulesEntry")?.value) || DEFAULT_RULESET.entryThreshold;
  const exit = Number(document.getElementById("rulesExit")?.value) || DEFAULT_RULESET.exitThreshold;
  const tp = (Number(document.getElementById("rulesTp")?.value) || DEFAULT_RULESET.takeProfitPct * 100) / 100;
  const vol = document.getElementById("rulesVol")?.value;
  const cooldown = Number(document.getElementById("rulesCooldown")?.value) || DEFAULT_RULESET.addCooldownDays;
  const fees = (document.getElementById("rulesFees")?.value || "on") === "on";

  return {
    entryThreshold: entry,
    exitThreshold: exit,
    takeProfitPct: tp,
    volThreshold: vol ? Number(vol) : null,
    addCooldownDays: cooldown,
    feesEnabled: fees,
  };
}

function buildFees(feesEnabled) {
  if (!feesEnabled) return { perTradePct: 0, slippagePct: 0 };
  return { perTradePct: 0.001, slippagePct: 0.0002 };
}

export function renderRulesBacktest(series, initialResult) {
  const root = document.getElementById("simpleRulesBacktest");
  if (!root) return;
  if (!Array.isArray(series) || !series.length) {
    root.innerHTML = `<div class="section-title flex"><span>Rule Simulation (Backtest)</span><span class="chip muted">Missing data</span></div><div class="muted">probability_daily.json missing.</div>`;
    return;
  }

  root.innerHTML = `
    <div class="section-title flex">
      <span>Rule Simulation (Backtest)</span>
      <span class="chip muted">Research only · Not advice</span>
    </div>
    <div class="subtitle">BUY/ADD/SELL driven by simple rules. Uses next-close execution, long-only, with optional fees.</div>
    ${controlsHtml()}
    <div class="kpis" id="rulesKpis"></div>
    <div id="rulesEquityChart" style="height:280px; margin-top:12px;"></div>
    <div class="section-title" style="margin-top:12px;">Trades</div>
    <table class="table">
      <thead><tr><th>Entry</th><th>Exit</th><th>Hold</th><th>Return</th><th>Reason</th><th>Size</th></tr></thead>
      <tbody id="rulesTrades"></tbody>
    </table>
    <details style="margin-top:12px;">
      <summary>Glossary</summary>
      <ul class="plain-list">
        <li>P(up): Probability of gain over horizon (model-derived).</li>
        <li>similarCount: Number of similar historical states (effective sample size).</li>
        <li>vol20: 20-day volatility (std of daily returns). Threshold = 70th percentile fallback.</li>
        <li>Execution: Decision on day t executes on close of t+1 (no lookahead).</li>
        <li>Fees: 0.10% per trade + 0.02% slippage when enabled.</li>
      </ul>
    </details>
    <div class="action-disclaimer" style="margin-top:12px;">
      <span class="warning-icon" aria-hidden="true">⚠️</span>
      <span>${DISCLAIMER}</span>
    </div>
  `;

  const kpiContainer = root.querySelector("#rulesKpis");
  const tradesContainer = root.querySelector("#rulesTrades");

  function runOnce(seedResult = null) {
    const params = readRulesetFromUi();
    const ruleset = {
      ...DEFAULT_RULESET,
      entryThreshold: params.entryThreshold,
      exitThreshold: params.exitThreshold,
      takeProfitPct: params.takeProfitPct,
      volThreshold: params.volThreshold,
      addCooldownDays: params.addCooldownDays,
    };
    const fees = buildFees(params.feesEnabled);
    const result = seedResult || backtestRuleset(series, ruleset, { fees });
    renderKpis(kpiContainer, result);
    renderTrades(tradesContainer, result.trades);
    plotEquity(result.timeline, result.benchmark);
    attachTooltips(root);
    return result;
  }

  const first = initialResult && initialResult.timeline?.length ? initialResult : null;
  runOnce(first);

  root.querySelector("#rulesRun")?.addEventListener("click", () => {
    root.querySelector("#rulesTrades").innerHTML = `<tr><td colspan="6" class="muted">Running…</td></tr>`;
    setTimeout(() => runOnce(), 20);
  });
}
