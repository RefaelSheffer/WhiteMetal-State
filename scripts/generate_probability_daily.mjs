import fs from "fs";
import path from "path";
import { buildFeatureRows, buildNormalizer, applyNormalizer } from "../src/lib/probability-engine.js";

const DEFAULT_FEATURES = ["ret_5", "ret_20", "vol_20", "rsi_14", "z_ma20"];

function validRowForFeatures(row, featureList) {
  if (!row || !row.f) return false;
  return featureList.every((key) => typeof row.f[key] === "number" && Number.isFinite(row.f[key]));
}

function euclideanDistance(a, b) {
  if (!a || !b || a.length !== b.length) return null;
  let sum = 0;
  for (let i = 0; i < a.length; i++) {
    const diff = a[i] - b[i];
    sum += diff * diff;
  }
  return Math.sqrt(sum / a.length);
}

function weightVector(distances) {
  const eps = 1e-6;
  const inv = distances.map((d) => 1 / (d + eps));
  const total = inv.reduce((a, b) => a + b, 0) || 1;
  return inv.map((w) => w / total);
}

function confidenceLabel(effectiveN, avgDistance) {
  if (effectiveN > 80 && avgDistance < 1.0) return "HIGH";
  if (effectiveN > 40 || avgDistance < 1.4) return "MEDIUM";
  return "LOW";
}

function computeProbability(rows, queryIndex, options = {}) {
  const featureList = options.features?.length ? options.features : DEFAULT_FEATURES;
  const horizon = options.horizon ?? 5;
  const k = options.k ?? 120;
  const targetKey = `fwd_${horizon}`;

  const queryRow = rows[queryIndex];
  if (!validRowForFeatures(queryRow, featureList)) return null;

  const labeledRows = [];
  for (let i = 0; i < queryIndex; i++) {
    if (i + horizon > queryIndex) continue;
    const row = rows[i];
    if (!validRowForFeatures(row, featureList)) continue;
    const label = row?.y?.[targetKey];
    if (typeof label !== "number" || !Number.isFinite(label)) continue;
    labeledRows.push(row);
  }

  if (!labeledRows.length) return null;

  const normalizer = buildNormalizer(labeledRows, featureList);
  const queryVector = applyNormalizer(queryRow, normalizer);
  if (!queryVector) return null;

  const candidates = labeledRows
    .map((row) => {
      const vec = applyNormalizer(row, normalizer);
      if (!vec) return null;
      const dist = euclideanDistance(queryVector, vec);
      if (dist === null) return null;
      return { dist, label: row.y[targetKey] };
    })
    .filter(Boolean)
    .sort((a, b) => a.dist - b.dist)
    .slice(0, k);

  if (!candidates.length) return null;

  const distances = candidates.map((c) => c.dist);
  const weights = weightVector(distances);
  const weightedProb = candidates.reduce((acc, c, idx) => acc + weights[idx] * (c.label > 0 ? 1 : 0), 0);
  const effN = 1 / weights.reduce((acc, w) => acc + w * w, 0);
  const avgDistance = candidates.reduce((acc, c, idx) => acc + c.dist * weights[idx], 0);

  return {
    pUp: weightedProb,
    confidence: confidenceLabel(effN, avgDistance),
    effectiveN: effN,
    avgDistance,
  };
}

function main() {
  const inputPath = path.join(process.cwd(), "public/data/raw/slv_daily.json");
  const outputPath = path.join(process.cwd(), "public/data/probability_daily.json");
  const raw = JSON.parse(fs.readFileSync(inputPath, "utf8"));
  const dates = raw.map((r) => r.date);
  const closes = raw.map((r) => r.close);

  const rows = buildFeatureRows(dates, closes, "SLV");
  const horizon = 5;
  const output = [];
  for (let i = 0; i < rows.length; i++) {
    const prob = computeProbability(rows, i, { horizon, k: 120 });
    output.push({
      date: dates[i],
      close: closes[i],
      pUp: prob ? Number(prob.pUp.toFixed(4)) : null,
      confidence: prob?.confidence || "LOW",
      effectiveN: prob ? Number(prob.effectiveN.toFixed(2)) : null,
      avgDistance: prob ? Number(prob.avgDistance.toFixed(3)) : null,
      horizon,
    });
  }

  fs.writeFileSync(outputPath, JSON.stringify(output, null, 2));
  console.log(`Wrote ${output.length} rows to ${outputPath}`);
}

main();
