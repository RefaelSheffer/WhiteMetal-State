// src/ui/render-action-recommendation.js
import { attachTooltips } from "./tooltips.js";

function escapeHtml(str) {
  return (str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function humanize(label) {
  if (!label) return "";
  return label.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function normalizeConfidence(raw) {
  if (typeof raw === "number") {
    if (raw >= 0.66) return "HIGH";
    if (raw >= 0.33) return "MEDIUM";
    return "LOW";
  }
  if (!raw) return "—";
  return String(raw).trim().toUpperCase();
}

function normalizeAction(raw) {
  if (!raw) return null;
  const value = String(raw).trim().toUpperCase();
  if (value.includes("BUY") && value.includes("ADD")) return "BUY / ADD";
  if (value.includes("SELL")) return "SELL";
  if (value.includes("ADD")) return "ADD";
  if (value.includes("BUY")) return "BUY";
  if (value.includes("HOLD")) return "HOLD";
  return value || null;
}

function normalizeBreakdown(breakdown) {
  if (Array.isArray(breakdown)) {
    return breakdown
      .map((b) => ({
        name: b?.name || humanize(b?.key || ""),
        points: b?.points ?? b?.score ?? null,
        summary: b?.summary || "",
      }))
      .filter((b) => b.name);
  }
  if (breakdown && typeof breakdown === "object") {
    return Object.entries(breakdown).map(([name, points]) => ({
      name: humanize(name),
      points: typeof points === "object" ? points.points ?? points.score ?? null : points,
      summary: typeof points === "object" ? points.summary || "" : "",
    }));
  }
  return [];
}

function normalizeSignalData(raw) {
  if (!raw || typeof raw !== "object") throw new Error("Invalid signal payload (expected an object)");

  const scoreTotal = raw.scoreTotal ?? raw.score_total ?? raw.total_score;
  const action = normalizeAction(raw.action || raw.recommendation || raw.signal?.action);
  if (scoreTotal === undefined || scoreTotal === null || isNaN(Number(scoreTotal))) {
    throw new Error("Invalid schema: missing scoreTotal/score_total");
  }
  if (!action) {
    throw new Error("Invalid schema: missing action");
  }

  const price = typeof raw.price === "number"
    ? raw.price
    : raw.price?.last_close ?? raw.price?.close ?? raw.price?.value ?? null;

  const asOf = raw.asOf || raw.updated_at_utc || raw.updated_at || raw.date || null;
  const scoreBreakdown = normalizeBreakdown(raw.scoreBreakdown || raw.breakdown);

  return {
    ticker: raw.ticker || raw.symbol || "—",
    asOf,
    action,
    price,
    scoreTotal: Number(scoreTotal),
    confidence: normalizeConfidence(raw.confidence ?? raw.confidence_score),
    scoreBreakdown,
    explainHeadline: raw.explain?.headline || raw.headline || "",
    explainBullets: Array.isArray(raw.explain?.bullets) ? raw.explain.bullets : [],
    disclaimer: raw.disclaimer || "Research only — not investment advice. Signals may be wrong.",
  };
}

function toneClass(action) {
  if (!action) return "neutral";
  if (action.includes("SELL")) return "negative";
  if (action.includes("BUY") || action.includes("ADD")) return "positive";
  return "neutral";
}

function fmtDate(asOf) {
  if (!asOf) return "—";
  try {
    const d = new Date(asOf);
    if (Number.isNaN(d.getTime())) return String(asOf).slice(0, 10);
    return d.toISOString().slice(0, 10);
  } catch (e) {
    return String(asOf).slice(0, 10);
  }
}

function fmtNumber(n, decimals = 2) {
  if (n === undefined || n === null || Number.isNaN(Number(n))) return "—";
  return Number(n).toFixed(decimals);
}

function renderBreakdownList(items) {
  if (!items.length) {
    return `<div class="muted">No score breakdown provided.</div>`;
  }
  return `
    <div class="action-breakdown">
      ${items.map((item) => `
        <div class="breakdown-row">
          <div class="breakdown-name">${escapeHtml(item.name || "")}</div>
          <div class="breakdown-points">${item.points !== undefined && item.points !== null ? `${item.points}` : "—"}</div>
          <div class="breakdown-summary">${escapeHtml(item.summary || "")}</div>
        </div>
      `).join("")}
    </div>
  `;
}

function renderError(root, error) {
  const attempts = Array.isArray(error?.details) ? error.details : null;
  const attemptsHtml = attempts && attempts.length
    ? `<ul>${attempts.map((a) => `<li><code>${escapeHtml(a.url)}</code> · ${escapeHtml(String(a.status))} · ${escapeHtml(a.errorText || "")}</li>`).join("")}</ul>`
    : escapeHtml(error?.details ? JSON.stringify(error.details, null, 2) : "");

  root.innerHTML = `
    <div class="section-title flex">
      <span>Action Recommendation</span>
      <span class="chip muted">Error</span>
    </div>
    <div class="alert-card">
      <div class="title">Failed to load signal_latest.json</div>
      <div class="muted">${escapeHtml(error?.message || "Unknown error")}</div>
      <details style="margin-top:8px;">
        <summary>Details</summary>
        <div class="muted" style="font-size:13px;">${attemptsHtml || "No fetch attempts recorded."}</div>
      </details>
    </div>
  `;
}

function renderLoading(root) {
  root.innerHTML = `
    <div class="section-title flex">
      <span>Action Recommendation</span>
      <span class="chip muted">Loading…</span>
    </div>
    <div class="muted">Loading action signal…</div>
  `;
}

export function renderActionRecommendation(signalData, error) {
  const root = document.getElementById("action-recommendation");
  if (!root) return;

  if (error) { renderError(root, error); attachTooltips(root); return; }
  if (!signalData) { renderLoading(root); attachTooltips(root); return; }

  let normalized;
  try {
    normalized = normalizeSignalData(signalData);
  } catch (e) {
    renderError(root, { message: e.message });
    attachTooltips(root);
    return;
  }

  const tone = toneClass(normalized.action);
  const breakdownHtml = renderBreakdownList(normalized.scoreBreakdown);
  const explainHtml = normalized.explainBullets.length
    ? `<ul class="evidence-list">${normalized.explainBullets.map((b) => `<li>${escapeHtml(b)}</li>`).join("")}</ul>`
    : `<div class="muted">No additional rationale provided.</div>`;

  root.innerHTML = `
    <div class="section-title flex">
      <span>Action Recommendation — ${escapeHtml(normalized.ticker)}</span>
      <span class="action-badge ${tone}">${escapeHtml(normalized.action)}</span>
    </div>
    <div class="action-meta">
      <div><span class="muted">As of</span> <span class="mono">${fmtDate(normalized.asOf)}</span></div>
      <div><span class="muted">Price</span> <span class="mono">${fmtNumber(normalized.price)}</span></div>
    </div>
    <div class="action-score-row">
      <div class="score-block">
        <div class="label" data-tooltip="Composite score out of 100">Total Score</div>
        <div class="value mono">${fmtNumber(normalized.scoreTotal, 0)}/100</div>
      </div>
      <div class="score-block">
        <div class="label" data-tooltip="Self-reported confidence level">Confidence</div>
        <div class="value mono">${escapeHtml(normalized.confidence)}</div>
      </div>
    </div>
    <details class="action-breakdown-panel" open>
      <summary>Score breakdown</summary>
      ${breakdownHtml}
    </details>
    <details class="action-rationale">
      <summary>Show full rationale</summary>
      <div class="rationale-headline">${escapeHtml(normalized.explainHeadline || "Signal explanation")}</div>
      ${explainHtml}
    </details>
    <details class="action-backtest" open>
      <summary>What-if (backtest lite)</summary>
      <div class="muted">Backtest coming soon: will simulate acting on BUY/ADD/SELL/HOLD over time.</div>
    </details>
    <div class="action-disclaimer">
      <span class="warning-icon" aria-hidden="true">⚠️</span>
      <span>${escapeHtml(normalized.disclaimer)}</span>
    </div>
  `;

  attachTooltips(root);
}
