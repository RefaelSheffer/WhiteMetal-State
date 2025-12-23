// src/ui/render-rules-recommendation.js
import { attachTooltips } from "./tooltips.js";

const DISCLAIMER = "This is an educational research tool. Rules-based signals and backtests are simplified and may be wrong. Not investment advice.";

function fmtPct(value, decimals = 0) {
  const num = toNumberOrNull(value);
  if (num === null) return "—";
  return `${(Number(num) * 100).toFixed(decimals)}%`;
}

function fmtNum(value, decimals = 2) {
  const num = toNumberOrNull(value);
  if (num === null) return "—";
  return Number(num).toFixed(decimals);
}

function toNumberOrNull(x) {
  if (x === null || x === undefined) return null;
  if (typeof x === "string") {
    const trimmed = x.trim();
    if (["NaN", "nan", "", "null"].includes(trimmed)) return null;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  if (typeof x === "number") {
    return Number.isFinite(x) ? x : null;
  }
  return null;
}

function fmtValue(value, decimals = 3) {
  const num = toNumberOrNull(value);
  if (num === null) {
    if (typeof value === "string" && value.trim()) return value.trim();
    return "—";
  }
  return fmtNum(num, decimals);
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
        .map((c) => {
          const status = (c.status || (c.pass ? "PASS" : "FAIL")).toUpperCase();
          const statusClass = status === "PASS" ? "pass" : status === "NA" ? "na" : "fail";
          const marker = status === "PASS" ? "✅" : status === "NA" ? "—" : "❌";
          const valueCopy = fmtValue(c.value, 3);
          const thresholdCopy =
            c.threshold !== undefined && c.threshold !== null ? fmtValue(c.threshold, 3) : null;
          const detailParts = [`Value: ${valueCopy}`];
          if (thresholdCopy) {
            const op = c.op ? ` (${c.op})` : "";
            detailParts.push(`Threshold ${thresholdCopy}${op}`);
          }
          if (c.note) detailParts.push(c.note);
          if (status === "NA" && c.missing_reason) detailParts.push(`Missing: ${c.missing_reason}`);
          return `
          <div class="rule-check ${statusClass}">
            <span class="marker">${marker}</span>
            <div>
              <div class="name">${c.label || c.name}</div>
              <div class="small muted">${detailParts.join(" · ")}</div>
            </div>
          </div>
        `;
        })
        .join("")}
    </div>
  `;
}

export function renderRulesRecommendation(result, error, lang = "en") {
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
      <div><span class="muted" data-glossary="probability_daily">P(up)</span> <span class="mono">${fmtPct(values.pUp, 1)}</span></div>
      <div><span class="muted" data-glossary="confidence_level">Confidence</span> <span class="mono">${fmtValue(values.confidence)}</span></div>
      <div><span class="muted" data-glossary="similar_states">Similar states</span> <span class="mono">${fmtNum(values.similarCount, 0)}</span></div>
      <div><span class="muted" data-glossary="vol20">Vol20</span> <span class="mono">${fmtNum(values.vol20, 3)}</span></div>
      <div><span class="muted" data-glossary="cot_bias">COT bias</span> <span class="mono">${cotBias}</span></div>
      <div><span class="muted" data-glossary="return_since_entry">Return vs entry</span> <span class="mono">${fmtPct(values.returnSinceEntry, 1)}</span></div>
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

  attachTooltips(root, lang);
}

export function renderRulesExplain(result, lang = "en") {
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
          <li data-glossary="probability_daily">P(up) ≥ ${fmtPct(r.entryThreshold, 0)}</li>
          <li data-glossary="confidence_level">Confidence ≥ ${r.minConfidence}</li>
          <li data-glossary="similar_states">Similar states ≥ ${r.minSimilarCount}</li>
          <li data-glossary="cot_tailwind">COT tailwind (commercial pct≤10% or bias bullish) lifts borderline entries.</li>
        </ul>
      </div>
      <div>
        <div class="subtitle">Exit</div>
        <ul class="plain-list">
          <li data-glossary="take_profit">Take profit: +${fmtPct(r.takeProfitPct, 0)}</li>
          <li data-glossary="probability_daily">P(up) < ${fmtPct(r.exitThreshold, 0)}</li>
          <li data-glossary="volatility_threshold">Vol spike ≥ ${fmtNum(r.volThreshold, 3)} (70th pct fallback)</li>
          <li data-glossary="cot_headwind">COT headwind can force SELL when longs are crowded.</li>
        </ul>
      </div>
      <div>
        <div class="subtitle">Add</div>
        <ul class="plain-list">
          <li data-glossary="probability_daily">P(up) ≥ ${fmtPct(r.addThreshold, 0)} & confidence HIGH</li>
          <li data-glossary="similar_states">Similar states ≥ ${r.addMinSimilarCount}</li>
          <li data-glossary="cooldown">Cooldown ${r.addCooldownDays}d · Add size ${(r.sizing?.add ?? 0.25) * 100}%</li>
          <li data-glossary="cot_support">COT support allows adds when commercials are stretched long.</li>
        </ul>
      </div>
    </div>
    <div class="muted" style="margin-top:8px;">Sizing: BUY ${(r.sizing?.buy ?? 0.5) * 100}% · ADD ${(r.sizing?.add ?? 0.25) * 100}% · SELL = flat.</div>
    <div class="action-disclaimer">
      <span class="warning-icon" aria-hidden="true">⚠️</span>
      <span>${DISCLAIMER}</span>
    </div>
  `;

  attachTooltips(root, lang);
}
