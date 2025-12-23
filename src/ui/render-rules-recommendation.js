// src/ui/render-rules-recommendation.js
import { attachTooltips } from "./tooltips.js";

const DISCLAIMER = "This is an educational research tool. Rules-based signals and backtests are simplified and may be wrong. Not investment advice.";

function fmtPct(value, decimals = 0) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return `${(Number(value) * 100).toFixed(decimals)}%`;
}

function fmtNum(value, decimals = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return Number(value).toFixed(decimals);
}

function toneClass(action) {
  if (!action) return "neutral";
  if (action.includes("SELL")) return "negative";
  if (action.includes("BUY") || action.includes("ADD")) return "positive";
  return "neutral";
}

function reasonCopy(reasonCode) {
  switch (reasonCode) {
    case "ENTRY_OK":
      return "P(up) is high enough with enough similar states and confidence → entry rule met.";
    case "TP":
      return "Take-profit reached (≥5% from entry).";
    case "PUP_DROP":
      return "P(up) dropped below the exit threshold.";
    case "VOL_SPIKE":
      return "Volatility spiked above the rule threshold.";
    case "ADD_STRENGTH":
      return "Existing position with strong, high-confidence setup → add on strength.";
    case "HOLD":
      return "No exit or add conditions met; hold position.";
    case "COT_SUPPORT":
      return "COT positioning is supportive (commercial washout / noncommercial fade).";
    case "COT_HEADWIND":
      return "COT positioning is a headwind; risk-off bias applied.";
    default:
      return "No trade rule met today.";
  }
}

function renderChecks(checks = []) {
  if (!checks.length) return "<div class=\"muted\">No checks computed.</div>";
  return `
    <div class="rule-checks">
      ${checks
        .map((c) => `
          <div class="rule-check ${c.pass ? "pass" : "fail"}">
            <span class="marker">${c.pass ? "✅" : "❌"}</span>
            <div>
              <div class="name">${c.name}</div>
              <div class="small muted">Value: ${fmtNum(c.value ?? c.value === 0 ? c.value : null, 3)}${
                c.threshold !== undefined && c.threshold !== null ? ` · Threshold ${fmtNum(c.threshold, 3)}` : ""
              }</div>
            </div>
          </div>
        `)
        .join("")}
    </div>
  `;
}

export function renderRulesRecommendation(result, error) {
  const root = document.getElementById("simpleRulesRecommendation");
  if (!root) return;

  if (error) {
    root.innerHTML = `<div class="section-title flex"><span>Action Recommendation (Rules)</span><span class="chip muted">Error</span></div><div class="alert-card">${error}</div>`;
    return;
  }

  if (!result || !result.latestDecision) {
    root.innerHTML = `<div class="section-title flex"><span>Action Recommendation (Rules)</span><span class="chip muted">Missing</span></div><div class="muted">No probability_daily.json or unable to compute rules.</div>`;
    return;
  }

  const decision = result.latestDecision;
  const tone = toneClass(decision.action);
  const values = decision.values || {};
  const cotBias = values.cotBias || "neutral";

  root.innerHTML = `
    <div class="section-title flex">
      <span>Action Recommendation (Rules-based, Research only)</span>
      <span class="action-badge ${tone}">${decision.action || "—"}</span>
    </div>
    <div class="action-meta">
      <div><span class="muted">Why</span> <span>${reasonCopy(decision.reasonCode)}</span></div>
      <div><span class="muted">P(up)</span> <span class="mono">${fmtPct(values.pUp, 1)}</span></div>
      <div><span class="muted">Confidence</span> <span class="mono">${values.confidence || "—"}</span></div>
      <div><span class="muted">Similar states</span> <span class="mono">${fmtNum(values.similarCount, 0)}</span></div>
      <div><span class="muted">Vol20</span> <span class="mono">${fmtNum(values.vol20, 3)}</span></div>
      <div><span class="muted">COT bias</span> <span class="mono">${cotBias}</span></div>
      <div><span class="muted">Return vs entry</span> <span class="mono">${fmtPct(values.returnSinceEntry, 1)}</span></div>
    </div>
    <details class="action-rationale" open>
      <summary>Today\'s checklist</summary>
      ${renderChecks([...(decision.checks?.entry || []), ...(decision.checks?.exit || []), ...(decision.checks?.add || []), ...(decision.checks?.cot || [])])}
    </details>
    <div class="action-disclaimer">
      <span class="warning-icon" aria-hidden="true">⚠️</span>
      <span>${DISCLAIMER}</span>
    </div>
  `;

  attachTooltips(root);
}

export function renderRulesExplain(result) {
  const root = document.getElementById("simpleRulesExplain");
  if (!root) return;

  if (!result || !result.rulesetUsed) {
    root.innerHTML = `<div class="section-title flex"><span>Rules (Explainable)</span><span class="chip muted">Missing</span></div><div class="muted">Ruleset not available.</div>`;
    return;
  }

  const r = result.rulesetUsed;

  root.innerHTML = `
    <div class="section-title flex">
      <span>Rules (Explainable)</span>
      <span class="chip muted">Entry / Exit / Add</span>
    </div>
    <div class="rules-grid">
      <div>
        <div class="subtitle">Entry</div>
        <ul class="plain-list">
          <li>P(up) ≥ ${fmtPct(r.entryThreshold, 0)}</li>
          <li>Confidence ≥ ${r.minConfidence}</li>
          <li>Similar states ≥ ${r.minSimilarCount}</li>
          <li>COT tailwind (commercial pct≤10% or bias bullish) lifts borderline entries.</li>
        </ul>
      </div>
      <div>
        <div class="subtitle">Exit</div>
        <ul class="plain-list">
          <li>Take profit: +${fmtPct(r.takeProfitPct, 0)}</li>
          <li>P(up) < ${fmtPct(r.exitThreshold, 0)}</li>
          <li>Vol spike ≥ ${fmtNum(r.volThreshold, 3)} (70th pct fallback)</li>
          <li>COT headwind can force SELL when longs are crowded.</li>
        </ul>
      </div>
      <div>
        <div class="subtitle">Add</div>
        <ul class="plain-list">
          <li>P(up) ≥ ${fmtPct(r.addThreshold, 0)} & confidence HIGH</li>
          <li>Similar states ≥ ${r.addMinSimilarCount}</li>
          <li>Cooldown ${r.addCooldownDays}d · Add size ${(r.sizing?.add ?? 0.25) * 100}%</li>
          <li>COT support allows adds when commercials are stretched long.</li>
        </ul>
      </div>
    </div>
    <div class="muted" style="margin-top:8px;">Sizing: BUY ${(r.sizing?.buy ?? 0.5) * 100}% · ADD ${(r.sizing?.add ?? 0.25) * 100}% · SELL = flat.</div>
    <div class="action-disclaimer">
      <span class="warning-icon" aria-hidden="true">⚠️</span>
      <span>${DISCLAIMER}</span>
    </div>
  `;

  attachTooltips(root);
}
