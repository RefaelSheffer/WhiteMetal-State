// src/lib/rules/ruleset-simple.js
import { attachVolatility, percentile } from "./volatility.js";

const CONFIDENCE_ORDER = { LOW: 0, MEDIUM: 1, HIGH: 2 };

function toFiniteNumber(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (["", "NaN", "nan", "null"].includes(trimmed)) return null;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  if (typeof value !== "number") return null;
  return Number.isFinite(value) ? value : null;
}

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
  const similarCountRaw =
    day.similarCount ?? day.similar_count ?? day.effectiveN ?? day.effective_n ?? null;
  const pUpRaw = day.pUp20 ?? day.pUp ?? day.p_up ?? null;
  const vol20Raw = day.vol20 ?? day.vol_20 ?? null;
  const confidence = String(day.confidence || "LOW").toUpperCase();
  const cotBias = day.cotBias ?? day.cot_bias ?? null;
  const cotCommercialNetPct52Raw =
    day.cotCommercialNetPct52 ?? day.cot_commercial_net_pct52 ?? null;
  const cotNoncommercialNetPct52Raw =
    day.cotNoncommercialNetPct52 ?? day.cot_noncommercial_net_pct52 ?? null;
  const cotCommercialNetZ52Raw = day.cotCommercialNetZ52 ?? day.cot_commercial_net_z52 ?? null;
  const cotNoncommercialNetZ52 =
    day.cotNoncommercialNetZ52 ?? day.cot_noncommercial_net_z52 ?? null;
  const cotOpenInterestRaw = day.cotOpenInterest ?? null;
  const close = toFiniteNumber(day.close);

  if (!day.date || close === null) return null;

  return {
    ...day,
    pUp: toFiniteNumber(pUpRaw),
    similarCount: toFiniteNumber(similarCountRaw),
    vol20: toFiniteNumber(vol20Raw),
    close,
    confidence,
    cotBias,
    cotCommercialNetPct52: toFiniteNumber(cotCommercialNetPct52Raw),
    cotNoncommercialNetPct52: toFiniteNumber(cotNoncommercialNetPct52Raw),
    cotCommercialNetZ52: toFiniteNumber(cotCommercialNetZ52Raw),
    cotNoncommercialNetZ52: toFiniteNumber(cotNoncommercialNetZ52),
    cotOpenInterest: toFiniteNumber(cotOpenInterestRaw),
  };
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

function buildCheck({ id, label, pass, value, threshold, op, note, missingReason }) {
  const normalizedValue = value === undefined ? null : value;
  const normalizedThreshold = threshold === undefined ? null : threshold;
  const hasData = normalizedValue !== null && normalizedThreshold !== null && Boolean(op);
  let status = "NA";
  let reason = missingReason || null;

  if (!hasData) {
    status = "NA";
    reason = reason || "no_data";
  } else if (typeof pass === "boolean") {
    status = pass ? "PASS" : "FAIL";
  } else {
    status = "FAIL";
  }

  return {
    id,
    label,
    pass: status === "PASS",
    status,
    value: normalizedValue,
    threshold: normalizedThreshold,
    op,
    note,
    missing_reason: reason,
  };
}

function buildNumericCheck({ id, label, value, threshold, op, note, missingReason }) {
  const safeValue = toFiniteNumber(value);
  const safeThreshold = toFiniteNumber(threshold);
  const hasData = safeValue !== null && safeThreshold !== null && Boolean(op);
  const pass = hasData
    ? op === ">="
      ? safeValue >= safeThreshold
      : op === "<="
        ? safeValue <= safeThreshold
        : op === ">"
          ? safeValue > safeThreshold
          : op === "<"
            ? safeValue < safeThreshold
            : op === "=="
              ? safeValue === safeThreshold
              : false
    : null;

  return buildCheck({
    id,
    label,
    pass,
    value: safeValue,
    threshold: safeThreshold,
    op,
    note,
    missingReason: hasData ? null : missingReason || "no_data",
  });
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
  const pUp = toFiniteNumber(normalized.pUp);
  const similarCount = normalized.similarCount;
  const vol20 = toFiniteNumber(normalized.vol20);
  const volThreshold = toFiniteNumber(rules.volThreshold ?? null);
  const entryPrice = toFiniteNumber(state.entryPrice ?? null);
  const close = toFiniteNumber(normalized.close);
  const returnSinceEntry = close !== null && entryPrice !== null ? close / entryPrice - 1 : null;

  const cotChecks = [
    buildNumericCheck({
      id: "cot_commercial_low",
      label: "Commercial washout (pct≤10%)",
      value: normalized.cotCommercialNetPct52,
      threshold: 0.1,
      op: "<=",
    }),
    buildNumericCheck({
      id: "cot_noncommercial_crowded",
      label: "Noncommercial crowded long (pct≥90%)",
      value: normalized.cotNoncommercialNetPct52,
      threshold: 0.9,
      op: ">=",
    }),
    buildCheck({
      id: "cot_bias_available",
      label: "COT bias available",
      pass: Boolean(normalized.cotBias),
      value: normalized.cotBias ?? null,
      threshold: "neutral",
      op: "exists",
      missingReason: normalized.cotBias ? null : "no_data",
    }),
  ];

  const entryChecks = [
    buildNumericCheck({
      id: "entry_pup",
      label: `P(up) ≥ ${(rules.entryThreshold * 100).toFixed(0)}%`,
      value: pUp,
      threshold: rules.entryThreshold,
      op: ">=",
    }),
    buildCheck({
      id: "entry_confidence",
      label: "Confidence MEDIUM/HIGH",
      pass: confidenceOk,
      value: confidence,
      threshold: rules.minConfidence,
      op: ">=",
    }),
    buildNumericCheck({
      id: "entry_similar_states",
      label: `Similar states ≥ ${rules.minSimilarCount}`,
      value: similarCount,
      threshold: rules.minSimilarCount,
      op: ">=",
    }),
    ...cotChecks,
  ];

  const exitChecks = [
    buildNumericCheck({
      id: "exit_take_profit",
      label: "Take profit +5%",
      value: returnSinceEntry,
      threshold: rules.takeProfitPct,
      op: ">=",
      missingReason: returnSinceEntry === null ? "no_data" : null,
    }),
    buildNumericCheck({
      id: "exit_pup_drop",
      label: `P(up) < ${(rules.exitThreshold * 100).toFixed(0)}%`,
      value: pUp,
      threshold: rules.exitThreshold,
      op: "<",
    }),
    buildNumericCheck({
      id: "exit_vol_spike",
      label: "Vol spike",
      value: vol20,
      threshold: volThreshold,
      op: ">=",
      note: "Vol ≥ threshold",
    }),
  ];

  const addChecks = [
    buildNumericCheck({
      id: "add_pup",
      label: `P(up) ≥ ${(rules.addThreshold * 100).toFixed(0)}%`,
      value: pUp,
      threshold: rules.addThreshold,
      op: ">=",
    }),
    buildCheck({
      id: "add_confidence",
      label: "Confidence HIGH",
      pass: confidenceRank(confidence) >= confidenceRank("HIGH"),
      value: confidence,
      threshold: "HIGH",
      op: ">=",
    }),
    buildNumericCheck({
      id: "add_similar_states",
      label: `Similar states ≥ ${rules.addMinSimilarCount}`,
      value: similarCount,
      threshold: rules.addMinSimilarCount,
      op: ">=",
    }),
    buildNumericCheck({
      id: "add_cooldown",
      label: "Cooldown passed",
      value:
        state.lastAddIndex === null || state.lastAddIndex === undefined
          ? rules.addCooldownDays
          : typeof state.index === "number" && typeof state.lastAddIndex === "number"
            ? state.index - state.lastAddIndex
            : null,
      threshold: rules.addCooldownDays,
      op: ">=",
      note: "Days since last add",
    }),
  ];

  let action = "NONE";
  let reasonCode = "NONE";
  const cotBullish = (normalized.cotBias || "").toLowerCase() === "bullish" || (typeof normalized.cotCommercialNetPct52 === "number" && normalized.cotCommercialNetPct52 <= 0.1);
  const cotBearish = (normalized.cotBias || "").toLowerCase() === "bearish" || (typeof normalized.cotNoncommercialNetPct52 === "number" && normalized.cotNoncommercialNetPct52 >= 0.9);

  const isPass = (check) => check?.status === "PASS";
  const entryPass = entryChecks.every((c) => isPass(c));
  const cotBoost = cotBullish && isPass(entryChecks[0]) && isPass(entryChecks[1]);
  if (state.positionPct > 0 || state.positionState === "LONG") {
    if (isPass(exitChecks[0])) { action = "SELL"; reasonCode = "TP"; }
    else if (isPass(exitChecks[1])) { action = "SELL"; reasonCode = "PUP_DROP"; }
    else if (isPass(exitChecks[2])) { action = "SELL"; reasonCode = "VOL_SPIKE"; }
    else if (cotBearish && isPass(exitChecks[1])) { action = "SELL"; reasonCode = "COT_HEADWIND"; }
    else if (addChecks.every((c) => isPass(c)) && !cotBearish) { action = "ADD"; reasonCode = "ADD_STRENGTH"; }
    else if (cotBullish && isPass(addChecks[0])) { action = "ADD"; reasonCode = "COT_SUPPORT"; }
    else { action = "HOLD"; reasonCode = "HOLD"; }
  } else {
    if (entryPass) { action = "BUY"; reasonCode = "ENTRY_OK"; }
    else if (cotBoost) { action = "BUY"; reasonCode = "COT_SUPPORT"; }
  }

  if (action === "BUY" && cotBearish && !cotBullish) {
    action = "HOLD";
    reasonCode = "COT_HEADWIND";
  }
  if (action === "ADD" && cotBearish) {
    action = "HOLD";
    reasonCode = "COT_HEADWIND";
  }

  return {
    action,
    reasonCode,
    checks: { entry: entryChecks, exit: exitChecks, add: addChecks, cot: cotChecks },
    values: {
      pUp,
      confidence,
      similarCount,
      vol20,
      returnSinceEntry,
      cotBias: normalized.cotBias,
      cotCommercialNetPct52: normalized.cotCommercialNetPct52,
      cotNoncommercialNetPct52: normalized.cotNoncommercialNetPct52,
      cotCommercialNetZ52: normalized.cotCommercialNetZ52,
      cotNoncommercialNetZ52: normalized.cotNoncommercialNetZ52,
      cotOpenInterest: normalized.cotOpenInterest,
    },
  };
}
