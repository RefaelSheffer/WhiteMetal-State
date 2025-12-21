// src/lib/rules/ruleset-simple.js
import { attachVolatility, percentile } from "./volatility.js";

const CONFIDENCE_ORDER = { LOW: 0, MEDIUM: 1, HIGH: 2 };

export const DEFAULT_RULESET = {
  entryThreshold: 0.6,
  exitThreshold: 0.5,
  takeProfitPct: 0.05,
  minConfidence: "MEDIUM",
  minSimilarCount: 100,
  volThreshold: null,
  volPercentile: 0.7,
  addThreshold: 0.65,
  addMinSimilarCount: 150,
  addCooldownDays: 10,
  sizing: { buy: 0.5, add: 0.25 },
  execution: "next_close",
  fees: { perTradePct: 0.001, slippagePct: 0.0002 },
};

function confidenceRank(label) {
  const key = String(label || "LOW").toUpperCase();
  return CONFIDENCE_ORDER[key] ?? 0;
}

function normalizeDay(day) {
  if (!day || typeof day !== "object") return null;
  const similarCount =
    day.similarCount ?? day.similar_count ?? day.effectiveN ?? day.effective_n ?? null;
  const pUp = day.pUp20 ?? day.pUp ?? day.p_up ?? null;
  const vol20 = day.vol20 ?? day.vol_20 ?? null;
  const confidence = String(day.confidence || "LOW").toUpperCase();

  if (!day.date || typeof day.close !== "number") return null;
  return { ...day, pUp, similarCount, vol20, confidence };
}

export function normalizeSeriesForRules(series) {
  if (!Array.isArray(series)) return [];
  const rows = series
    .map((d) => normalizeDay(d))
    .filter((d) => d && d.date && typeof d.close === "number")
    .sort((a, b) => (a.date > b.date ? 1 : -1));

  const withVol = attachVolatility(rows, 20);
  const volThreshold = percentile(withVol.map((r) => r.vol20).filter((v) => v !== null), 0.7);

  return { rows: withVol, derivedVolThreshold: volThreshold };
}

function buildCheck(name, pass, value, threshold, note) {
  return { name, pass, value, threshold, note };
}

function formatMissing(value) {
  return value === null || value === undefined ? "—" : value;
}

export function evaluateRules(day, state = {}, ruleset = DEFAULT_RULESET) {
  const normalized = normalizeDay(day);
  if (!normalized) {
    return { action: "NONE", reasonCode: "MISSING", checks: { entry: [], exit: [], add: [] } };
  }

  const rules = { ...DEFAULT_RULESET, ...ruleset };
  const minConfRank = confidenceRank(rules.minConfidence);
  const confidence = normalized.confidence || "LOW";
  const confidenceOk = confidenceRank(confidence) >= minConfRank;
  const pUp = normalized.pUp;
  const similarCount = normalized.similarCount ?? 0;
  const vol20 = normalized.vol20 ?? null;
  const volThreshold = rules.volThreshold ?? null;
  const returnSinceEntry = state.entryPrice
    ? normalized.close / state.entryPrice - 1
    : null;

  const entryChecks = [
    buildCheck(`P(up) ≥ ${(rules.entryThreshold * 100).toFixed(0)}%`, typeof pUp === "number" && pUp >= rules.entryThreshold, pUp, rules.entryThreshold),
    buildCheck("Confidence MEDIUM/HIGH", confidenceOk, confidence, rules.minConfidence),
    buildCheck(`Similar states ≥ ${rules.minSimilarCount}`, similarCount >= rules.minSimilarCount, similarCount, rules.minSimilarCount),
  ];

  const exitChecks = [
    buildCheck("Take profit +5%", typeof returnSinceEntry === "number" && returnSinceEntry >= rules.takeProfitPct, returnSinceEntry, rules.takeProfitPct),
    buildCheck(`P(up) < ${(rules.exitThreshold * 100).toFixed(0)}%`, typeof pUp === "number" && pUp < rules.exitThreshold, pUp, rules.exitThreshold),
    buildCheck("Vol spike", volThreshold !== null && vol20 !== null && vol20 >= volThreshold, formatMissing(vol20), volThreshold, "Vol ≥ threshold"),
  ];

  const addChecks = [
    buildCheck(`P(up) ≥ ${(rules.addThreshold * 100).toFixed(0)}%`, typeof pUp === "number" && pUp >= rules.addThreshold, pUp, rules.addThreshold),
    buildCheck("Confidence HIGH", confidenceRank(confidence) >= confidenceRank("HIGH"), confidence, "HIGH"),
    buildCheck(`Similar states ≥ ${rules.addMinSimilarCount}`, similarCount >= rules.addMinSimilarCount, similarCount, rules.addMinSimilarCount),
    buildCheck("Cooldown passed", state.lastAddIndex === null || state.lastAddIndex === undefined || (typeof state.index === "number" && typeof state.lastAddIndex === "number" ? state.index - state.lastAddIndex >= rules.addCooldownDays : true), state.lastAddIndex, rules.addCooldownDays, "Days since last add"),
  ];

  let action = "NONE";
  let reasonCode = "NONE";

  if (state.positionPct > 0 || state.positionState === "LONG") {
    if (exitChecks[0].pass) { action = "SELL"; reasonCode = "TP"; }
    else if (exitChecks[1].pass) { action = "SELL"; reasonCode = "PUP_DROP"; }
    else if (exitChecks[2].pass) { action = "SELL"; reasonCode = "VOL_SPIKE"; }
    else if (addChecks.every((c) => c.pass)) { action = "ADD"; reasonCode = "ADD_STRENGTH"; }
    else { action = "HOLD"; reasonCode = "HOLD"; }
  } else {
    if (entryChecks.every((c) => c.pass)) { action = "BUY"; reasonCode = "ENTRY_OK"; }
  }

  return {
    action,
    reasonCode,
    checks: { entry: entryChecks, exit: exitChecks, add: addChecks },
    values: { pUp, confidence, similarCount, vol20, returnSinceEntry },
  };
}
