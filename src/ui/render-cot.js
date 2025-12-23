// src/ui/render-cot.js
import { describeBias } from "../lib/cot.js";
import { attachTooltips } from "./tooltips.js";

function fmtNumber(n, decimals = 0) {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  return Number(n).toFixed(decimals);
}

function fmtPct(n, decimals = 0) {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  return `${(Number(n) * 100).toFixed(decimals)}%`;
}

function fmtDate(d) {
  if (!d) return "—";
  try {
    const dt = new Date(d);
    return Number.isNaN(dt.getTime()) ? String(d) : dt.toISOString().slice(0, 10);
  } catch (e) {
    return String(d);
  }
}

function renderGroupRow(label, group) {
  if (!group) return "";
  return `
    <div class="cot-row">
      <div class="cot-label">${label}</div>
      <div class="cot-value mono">Net ${fmtNumber(group.net)}</div>
      <div class="cot-sub muted">Δ1w ${fmtNumber(group.net_change_1w)} · Δ2w ${fmtNumber(group.net_change_2w)}</div>
      <div class="cot-sub muted">Z ${fmtNumber(group.z_52w, 2)} · Pctl ${fmtPct(group.pct_52w, 1)}</div>
    </div>
  `;
}

export function renderCotCard(latest, history, error) {
  const root = document.getElementById("cotCard");
  if (!root) return;
  if (error) {
    root.innerHTML = `<div class="section-title flex"><span>COT (Silver)</span><span class="chip muted">Error</span></div><div class="muted">${error.message || error}</div>`;
    return;
  }
  if (!latest) {
    root.innerHTML = `<div class="section-title flex"><span>COT (Silver)</span><span class="chip muted">Loading…</span></div>`;
    return;
  }

  const bias = describeBias(latest?.signals?.cot_bias || "neutral");
  const comm = latest?.groups?.commercial;
  const nonc = latest?.groups?.noncommercial;
  const oi = latest?.open_interest;

  root.innerHTML = `
    <div class="section-title flex">
      <span>COT (Silver)</span>
      <span class="chip ${bias.tone}">${bias.label}</span>
    </div>
    <div class="muted" style="margin-bottom:6px;">As of ${fmtDate(latest.as_of)} · ${latest.market || "COMEX Silver"}</div>
    <div class="cot-grid">
      ${renderGroupRow("Commercial", comm)}
      ${renderGroupRow("Noncommercial", nonc)}
      <div class="cot-row">
        <div class="cot-label">Open interest</div>
        <div class="cot-value mono">${fmtNumber(oi)}</div>
        <div class="cot-sub muted">Δ4w ${fmtNumber(latest?.open_interest_change_4w)}</div>
      </div>
    </div>
    <div class="cot-foot">
      <div class="muted small">Confidence: ${latest?.signals?.confidence || "—"}</div>
      <div class="muted small">Reasons: ${(latest?.signals?.reason || []).join(", ") || "—"}</div>
      <div class="muted small">Updated: ${fmtDate(latest?.last_updated_utc)}</div>
      <div class="muted small">Source: ${latest?.source?.provider || "CFTC"}</div>
    </div>
  `;
  attachTooltips(root);
}
