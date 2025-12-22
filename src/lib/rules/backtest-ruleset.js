// src/lib/rules/backtest-ruleset.js
import { summarizeKpis, calcMaxDrawdown } from "../trading/metrics.js";
import { DEFAULT_RULESET, evaluateRules, normalizeSeriesForRules } from "./ruleset-simple.js";
import { percentile } from "./volatility.js";

// Re-export the default ruleset so UI modules can rely on a single import source.
export { DEFAULT_RULESET } from "./ruleset-simple.js";

function clone(obj) {
  return typeof structuredClone === "function" ? structuredClone(obj) : JSON.parse(JSON.stringify(obj || {}));
}

function computeVolThreshold(rows, fallback) {
  const vols = rows.map((r) => r.vol20).filter((v) => typeof v === "number" && Number.isFinite(v));
  return vols.length ? percentile(vols, 0.7) : fallback ?? null;
}

function equityForPrice(cash, shares, price) {
  return cash + shares * price;
}

export function backtestRuleset(series, ruleset = DEFAULT_RULESET, config = {}) {
  const { rows, derivedVolThreshold } = normalizeSeriesForRules(series);
  if (!rows.length) {
    return {
      timeline: [],
      trades: [],
      kpis: {},
      benchmark: {},
      latestDecision: null,
      rulesetUsed: { ...DEFAULT_RULESET, ...ruleset },
    };
  }

  const rules = clone({ ...DEFAULT_RULESET, ...ruleset });
  const initialCapital = config.initialCapital ?? 10000;
  const feeRate = (config.fees?.perTradePct ?? rules.fees?.perTradePct ?? 0.001);
  const slippage = (config.fees?.slippagePct ?? rules.fees?.slippagePct ?? 0.0002);
  const execution = config.execution || rules.execution || "next_close";

  if (rules.volThreshold === null || rules.volThreshold === undefined) {
    rules.volThreshold = derivedVolThreshold ?? computeVolThreshold(rows, null);
  }

  const timeline = [];
  const trades = [];
  let cashNet = initialCapital;
  let cashGross = initialCapital;
  let sharesNet = 0;
  let sharesGross = 0;
  let positionState = "FLAT";
  let positionPct = 0;
  let entryPriceNet = null;
  let entryPriceGross = null;
  let entryDate = null;
  let entryIndex = null;
  let lastAddIndex = null;
  let totalFees = 0;
  let pendingAction = null;
  let latestDecision = null;

  const executeTrade = (action, idx, reasonCode) => {
    const bar = rows[idx];
    if (!bar) return;
    const priceGross = bar.close;
    const priceNet = action === "SELL" ? priceGross * (1 - slippage) : priceGross * (1 + slippage);

    if (action === "BUY" || action === "ADD") {
      const addFraction = action === "BUY" ? rules.sizing?.buy ?? 0.5 : rules.sizing?.add ?? 0.25;
      const equityNet = equityForPrice(cashNet, sharesNet, priceNet);
      const equityGross = equityForPrice(cashGross, sharesGross, priceGross);
      const targetFraction = Math.min(addFraction, Math.max(0, 1 - positionPct));
      if (targetFraction <= 0) return;
      const allocationNet = Math.min(equityNet * targetFraction, cashNet);
      const allocationGross = Math.min(equityGross * targetFraction, cashGross);
      if (allocationNet <= 0 || allocationGross <= 0) return;
      const fee = allocationNet * feeRate;
      const sharesPurchasedNet = allocationNet / priceNet;
      const sharesPurchasedGross = allocationGross / priceGross;

      const prevSharesNet = sharesNet;
      const prevSharesGross = sharesGross;

      sharesNet += sharesPurchasedNet;
      sharesGross += sharesPurchasedGross;
      cashNet -= allocationNet + fee;
      cashGross -= allocationGross;
      totalFees += fee;

      const positionValueNet = sharesNet * priceNet;
      const positionValueGross = sharesGross * priceGross;
      entryPriceNet = prevSharesNet > 0
        ? (entryPriceNet * prevSharesNet + priceNet * sharesPurchasedNet) / sharesNet
        : priceNet;
      entryPriceGross = prevSharesGross > 0
        ? (entryPriceGross * prevSharesGross + priceGross * sharesPurchasedGross) / sharesGross
        : priceGross;

      positionPct = positionValueNet && equityNet ? positionValueNet / (cashNet + positionValueNet) : 0;
      positionState = "LONG";
      if (action === "BUY") {
        entryDate = bar.date;
        entryIndex = idx;
      }
      if (action === "ADD") {
        lastAddIndex = idx;
      }
    }

    if (action === "SELL" && positionState === "LONG") {
      const proceedsGross = sharesGross * priceGross;
      const proceedsNet = sharesNet * priceNet;
      const fee = proceedsNet * feeRate;
      cashGross += proceedsGross;
      cashNet += proceedsNet - fee;
      totalFees += fee;
      const grossReturnPct = entryPriceGross ? priceGross / entryPriceGross - 1 : 0;
      const netReturnPct = entryPriceNet ? priceNet / entryPriceNet - 1 : 0;
      const holdingDays = entryIndex !== null ? idx - entryIndex : 0;
      trades.push({
        entryDate,
        exitDate: bar.date,
        entryPrice: entryPriceGross,
        exitPrice: priceNet,
        sizePct: positionPct,
        grossReturnPct,
        netReturnPct,
        holdingDays,
        exitReason: reasonCode || "RULE_EXIT",
      });
      sharesNet = 0;
      sharesGross = 0;
      positionPct = 0;
      positionState = "FLAT";
      entryPriceNet = null;
      entryPriceGross = null;
      entryDate = null;
      entryIndex = null;
      lastAddIndex = null;
    }
  };

  for (let i = 0; i < rows.length; i++) {
    if (pendingAction && pendingAction.executeIdx === i) {
      executeTrade(pendingAction.action, i, pendingAction.reasonCode);
      pendingAction = null;
    }

    const bar = rows[i];
    const equityNet = equityForPrice(cashNet, sharesNet, bar.close);
    const equityGross = equityForPrice(cashGross, sharesGross, bar.close);
    positionPct = equityNet ? (sharesNet * bar.close) / equityNet : 0;

    const decision = evaluateRules(bar, {
      positionPct,
      positionState,
      entryPrice: entryPriceNet,
      daysInTrade: entryIndex !== null ? i - entryIndex : 0,
      lastAddIndex,
      index: i,
    }, rules);

    latestDecision = decision;

    timeline.push({
      date: bar.date,
      close: bar.close,
      equityNet,
      equityGross,
      positionPct,
      action: decision.action,
      reasonCode: decision.reasonCode,
    });

    const actionable = decision.action === "BUY" || decision.action === "ADD" || decision.action === "SELL";
    if (actionable) {
      const execIdx = execution === "next_close" && i + 1 < rows.length ? i + 1 : i;
      if (pendingAction && pendingAction.executeIdx === execIdx) {
        pendingAction = { action: decision.action, executeIdx: execIdx, reasonCode: decision.reasonCode };
      } else {
        pendingAction = { action: decision.action, executeIdx: execIdx, reasonCode: decision.reasonCode };
      }
    }
  }

  if (pendingAction) {
    executeTrade(pendingAction.action, Math.min(pendingAction.executeIdx, rows.length - 1), pendingAction.reasonCode);
  }

  if (positionState === "LONG" && sharesNet > 0) {
    executeTrade("SELL", rows.length - 1, "FORCED_EXIT_END_OF_DATA");
  }

  const startDate = rows[0].date;
  const endDate = rows[rows.length - 1].date;
  const buyHoldStart = rows[0].close;
  const buyHoldCurve = rows.map((r) => ({ date: r.date, equity: buyHoldStart ? (r.close / buyHoldStart) * initialCapital : initialCapital }));
  const kpis = summarizeKpis({ timeline, trades, startDate, endDate });

  return {
    timeline,
    trades,
    kpis,
    benchmark: {
      buyHoldTotalReturnPct: buyHoldStart ? rows[rows.length - 1].close / buyHoldStart - 1 : 0,
      buyHoldMaxDrawdownPct: calcMaxDrawdown(buyHoldCurve.map((r) => r.equity)),
      buyHold: buyHoldCurve,
    },
    latestDecision,
    rulesetUsed: rules,
    fees: { totalFeesPaid: totalFees, feeRate, slippage },
  };
}
