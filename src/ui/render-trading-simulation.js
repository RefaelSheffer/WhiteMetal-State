// src/ui/render-trading-simulation.js
import { runTradingSimulation } from "../lib/trading/trade-engine.js";

const DEFAULT_PARAMS = {
  entryThreshold: 0.6,
  exitThreshold: 0.5,
  exitConfirmDays: 0,
  maxHoldDays: 20,
  stopLossPct: 5,
  trailingStopPct: 0,
  minConfidence: "MEDIUM",
  sizingMode: "all_in",
  sizingFraction: 1,
  perTradePct: 0.1,
  slippagePct: 0.02,
  execution: "next_close",
  initialCapital: 10000,
};

const CONFIDENCE_OPTIONS = ["LOW", "MEDIUM", "HIGH"];

function fmtPct(value, decimals = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return `${(Number(value) * 100).toFixed(decimals)}%`;
}

function fmtNum(value, decimals = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return Number(value).toFixed(decimals);
}

function buildInputs(params) {
  return `
    <div class="settings">
      <div>
        <label class="small">Entry Threshold (P(up))</label>
        <input type="number" step="0.01" min="0" max="1" id="ts-entry" value="${params.entryThreshold}" />
      </div>
      <div>
        <label class="small">Min confidence</label>
        <select id="ts-confidence">
          ${CONFIDENCE_OPTIONS.map((c) => `<option value="${c}" ${c === params.minConfidence ? "selected" : ""}>${c}</option>`).join("")}
        </select>
      </div>
      <div>
        <label class="small">Exit Threshold</label>
        <input type="number" step="0.01" min="0" max="1" id="ts-exit" value="${params.exitThreshold}" />
      </div>
      <div>
        <label class="small">Exit confirm days</label>
        <input type="number" min="0" max="10" step="1" id="ts-exit-confirm" value="${params.exitConfirmDays}" />
      </div>
      <div>
        <label class="small">Max hold (days)</label>
        <input type="number" min="1" max="120" step="1" id="ts-max-hold" value="${params.maxHoldDays}" />
      </div>
      <div>
        <label class="small">Stop loss (%)</label>
        <input type="number" min="0" max="50" step="0.5" id="ts-stop" value="${params.stopLossPct}" />
      </div>
      <div>
        <label class="small">Trailing stop (%)</label>
        <input type="number" min="0" max="50" step="0.5" id="ts-trail" value="${params.trailingStopPct}" />
      </div>
      <div>
        <label class="small">Sizing</label>
        <select id="ts-sizing">
          <option value="all_in" ${params.sizingMode === "all_in" ? "selected" : ""}>All in</option>
          <option value="fixed_frac" ${params.sizingMode === "fixed_frac" ? "selected" : ""}>Fixed fraction</option>
        </select>
      </div>
      <div>
        <label class="small">Fraction (0-1)</label>
        <input type="number" min="0" max="1" step="0.05" id="ts-fraction" value="${params.sizingFraction}" />
      </div>
      <div>
        <label class="small">Per-trade fee (%)</label>
        <input type="number" min="0" max="1" step="0.01" id="ts-fee" value="${params.perTradePct}" />
      </div>
      <div>
        <label class="small">Slippage (%)</label>
        <input type="number" min="0" max="1" step="0.01" id="ts-slip" value="${params.slippagePct}" />
      </div>
      <div>
        <label class="small">Initial capital</label>
        <input type="number" min="1000" step="500" id="ts-capital" value="${params.initialCapital}" />
      </div>
    </div>
    <div class="controls" style="margin-top:10px;">
      <button class="btn" id="ts-run">Run Trading Simulation</button>
      <button class="btn" id="ts-optimize">Optimize Strategy</button>
    </div>
  `;
}

function renderKpis(container, result) {
  const k = result?.kpis || {};
  const b = result?.benchmark || {};
  container.innerHTML = `
    <div class="kpis">
      <div class="kpi"><div class="muted" data-tooltip="תשואה מצטברת של האסטרטגיה לאורך כל התקופה">Net Return</div><div class="v mono">${fmtPct(k.totalReturnPct)}</div></div>
      <div class="kpi"><div class="muted" data-tooltip="CAGR: שיעור תשואה שנתית ממוצעת במונחים מצטברים">CAGR</div><div class="v mono">${fmtPct(k.cagrPct)}</div></div>
      <div class="kpi"><div class="muted" data-tooltip="Max Drawdown: הירידה החדה ביותר משיא לשפל">Max DD</div><div class="v mono">${fmtPct(k.maxDrawdownPct)}</div></div>
      <div class="kpi"><div class="muted" data-tooltip="Sharpe Ratio: תשואה עודפת חלקי סטיית תקן של התשואות">Sharpe</div><div class="v mono">${fmtNum(k.sharpe)}</div></div>
      <div class="kpi"><div class="muted" data-tooltip="Win rate: אחוז העסקאות שנסגרו ברווח">Win rate</div><div class="v mono">${fmtPct(k.winRatePct)}</div></div>
      <div class="kpi"><div class="muted" data-tooltip="מספר העסקאות שבוצעו בסימולציה"># Trades</div><div class="v mono">${k.tradesCount ?? 0}</div></div>
      <div class="kpi"><div class="muted" data-tooltip="Exposure: חלק מהזמן או מההון שהיה מושקע בשוק">Exposure</div><div class="v mono">${fmtPct(k.exposurePct)}</div></div>
    </div>
    <div class="muted" style="margin-top:8px;">Buy & Hold: ${fmtPct(b.buyHoldTotalReturnPct)} · MaxDD ${fmtPct(b.buyHoldMaxDrawdownPct)}</div>
  `;
}

function renderTradesTable(container, trades) {
  if (!Array.isArray(trades) || !trades.length) {
    container.innerHTML = `<tr><td colspan="6" class="muted">No trades</td></tr>`;
    return;
  }
  container.innerHTML = trades
    .slice(-20)
    .reverse()
    .map((t) => `
      <tr>
        <td>${t.entryDate}</td>
        <td>${t.exitDate}</td>
        <td>${t.holdingDays ?? 0}d</td>
        <td>${fmtPct(t.netReturnPct, 2)}</td>
        <td>${t.exitReason || "—"}</td>
        <td>${fmtNum(t.sizePct * 100, 1)}%</td>
      </tr>
    `)
    .join("");
}

function plotEquity(curve, benchmark) {
  const el = document.getElementById("ts-equity");
  if (!el) return;
  if (!curve?.length) {
    el.textContent = "No data";
    return;
  }
  const dates = curve.map((r) => r.date);
  const net = curve.map((r) => r.equityNet);
  const gross = curve.map((r) => r.equityGross);
  const buyHold = benchmark?.map((v, idx) => ({ x: dates[idx], y: v })) || [];

  const traces = [
    { x: dates, y: net, mode: "lines", name: "Strategy (Net)", line: { color: "#4c9cff" } },
    { x: dates, y: gross, mode: "lines", name: "Strategy (Gross)", line: { color: "#9ca3af", dash: "dot" } },
  ];
  if (buyHold.length) traces.push({ x: buyHold.map((p) => p.x), y: buyHold.map((p) => p.y), mode: "lines", name: "Buy & Hold", line: { color: "#fbbf24", dash: "dash" } });

  Plotly.newPlot(el, traces, { paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", margin: { l: 45, r: 10, t: 12, b: 40 }, xaxis: { color: "#a8b4d6" }, yaxis: { color: "#a8b4d6", gridcolor: "#1e2a45" } }, { displayModeBar: false });
}

function readParams() {
  return {
    entryThreshold: Number(document.getElementById("ts-entry")?.value) || DEFAULT_PARAMS.entryThreshold,
    exitThreshold: Number(document.getElementById("ts-exit")?.value) || DEFAULT_PARAMS.exitThreshold,
    exitConfirmDays: Number(document.getElementById("ts-exit-confirm")?.value) || 0,
    maxHoldDays: Number(document.getElementById("ts-max-hold")?.value) || DEFAULT_PARAMS.maxHoldDays,
    stopLossPct: Number(document.getElementById("ts-stop")?.value) || 0,
    trailingStopPct: Number(document.getElementById("ts-trail")?.value) || 0,
    minConfidence: document.getElementById("ts-confidence")?.value || DEFAULT_PARAMS.minConfidence,
    sizingMode: document.getElementById("ts-sizing")?.value || DEFAULT_PARAMS.sizingMode,
    sizingFraction: Number(document.getElementById("ts-fraction")?.value) || DEFAULT_PARAMS.sizingFraction,
    perTradePct: Number(document.getElementById("ts-fee")?.value) || DEFAULT_PARAMS.perTradePct,
    slippagePct: Number(document.getElementById("ts-slip")?.value) || DEFAULT_PARAMS.slippagePct,
    execution: "next_close",
    initialCapital: Number(document.getElementById("ts-capital")?.value) || DEFAULT_PARAMS.initialCapital,
  };
}

function scoreResult(result) {
  const cagr = result?.kpis?.cagrPct ?? 0;
  const maxDd = Math.abs(result?.kpis?.maxDrawdownPct ?? 0);
  return 0.6 * cagr - 0.4 * maxDd;
}

function runOptimization(series) {
  const entryRange = [0.55, 0.6, 0.65, 0.7];
  const exitRange = [0.45, 0.5, 0.55, 0.6];
  const holdOptions = [10, 20, 30];
  const stopOptions = [3, 5, 8];
  const confidenceOptions = ["MEDIUM", "HIGH"];

  const combos = [];
  entryRange.forEach((entryThreshold) => {
    exitRange.forEach((exitThreshold) => {
      holdOptions.forEach((maxHoldDays) => {
        stopOptions.forEach((stopLossPct) => {
          confidenceOptions.forEach((minConfidence) => {
            combos.push({ entryThreshold, exitThreshold, maxHoldDays, stopLossPct, minConfidence });
          });
        });
      });
    });
  });

  const results = combos.slice(0, 120).map((combo) => {
    const res = runTradingSimulation({ ...DEFAULT_PARAMS, ...combo }, series);
    return { combo, res, score: scoreResult(res) };
  });

  results.sort((a, b) => b.score - a.score);
  return results.slice(0, 10);
}

export function renderTradingSimulationCard(series) {
  const root = document.getElementById("tradingSimulationCard");
  if (!root) return;

  if (!Array.isArray(series) || !series.length) {
    root.innerHTML = `<div class="section-title">Trading Simulation (Research)</div><div class="muted">Missing probability_daily.json — unable to run.</div>`;
    return;
  }

  root.innerHTML = `
    <div class="section-title flex">
      <span>Trading Simulation (Research Backtest)</span>
      <span class="chip muted">Research only · Not advice</span>
    </div>
    <div class="subtitle">P(up)/confidence driven entry & exit. Uses next-close execution without lookahead.</div>
    ${buildInputs(DEFAULT_PARAMS)}
    <div class="kpis" id="ts-kpis"></div>
    <div id="ts-equity" style="height:280px; margin-top:12px;"></div>
    <div class="section-title" style="margin-top:12px;">Trades</div>
    <table class="table">
      <thead><tr><th>Entry</th><th>Exit</th><th>Hold</th><th>Return</th><th>Reason</th><th>Size</th></tr></thead>
      <tbody id="ts-trades"></tbody>
    </table>
    <details style="margin-top:12px;" open>
      <summary>Optimization (quick grid)</summary>
      <div class="muted" id="ts-opt-note">Quick search over entry/exit/hold/stop to surface top combos.</div>
      <table class="table">
        <thead><tr><th>Score</th><th>Entry</th><th>Exit</th><th>Hold</th><th>Stop</th><th>CAGR</th><th>MaxDD</th></tr></thead>
        <tbody id="ts-opt-body"><tr><td colspan="7" class="muted">Not run</td></tr></tbody>
      </table>
    </details>
    <div class="action-disclaimer" style="margin-top:12px;">
      <span class="warning-icon" aria-hidden="true">⚠️</span>
      <span>Research simulation only. Uses historical data and does not constitute investment advice.</span>
    </div>
  `;

  const kpiContainer = root.querySelector("#ts-kpis");
  const tradesBody = root.querySelector("#ts-trades");
  const optBody = root.querySelector("#ts-opt-body");

  function runOnce() {
    const params = readParams();
    const result = runTradingSimulation(params, series);
    renderKpis(kpiContainer, result);
    renderTradesTable(tradesBody, result.trades);
    plotEquity(result.timeline, result.equityCurves?.buyHold);
    return result;
  }

  runOnce();

  root.querySelector("#ts-run")?.addEventListener("click", () => {
    runOnce();
  });

  root.querySelector("#ts-optimize")?.addEventListener("click", () => {
    optBody.innerHTML = `<tr><td colspan="7" class="muted">Running…</td></tr>`;
    setTimeout(() => {
      const results = runOptimization(series);
      if (!results.length) {
        optBody.innerHTML = `<tr><td colspan="7" class="muted">No optimization results</td></tr>`;
        return;
      }
      optBody.innerHTML = results
        .map(({ combo, res, score }) => `
          <tr>
            <td>${fmtNum(score, 3)}</td>
            <td>${combo.entryThreshold}</td>
            <td>${combo.exitThreshold}</td>
            <td>${combo.maxHoldDays}d</td>
            <td>${combo.stopLossPct}%</td>
            <td>${fmtPct(res?.kpis?.cagrPct ?? 0, 2)}</td>
            <td>${fmtPct(res?.kpis?.maxDrawdownPct ?? 0, 2)}</td>
          </tr>
        `)
        .join("");
    }, 50);
  });
}
