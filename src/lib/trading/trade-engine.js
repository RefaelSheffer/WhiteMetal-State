// src/lib/trading/trade-engine.js
import { summarizeKpis, calcMaxDrawdown } from "./metrics.js";

function confidenceRank(label) {
  const order = { LOW: 0, MEDIUM: 1, HIGH: 2 };
  return order[String(label || "").toUpperCase()] ?? -1;
}

function normalizeSeries(series) {
  return Array.isArray(series)
    ? series
        .filter((row) => row && row.date && typeof row.close === "number")
        .map((row) => ({
          date: row.date,
          close: Number(row.close),
          pUp: row.pUp ?? row.p_up ?? null,
          confidence: String(row.confidence || "LOW").toUpperCase(),
        }))
    : [];
}

export function trade_engine_cycle_basic({
  series,
  entryRule,
  exitRule,
  stops = {},
  sizing = { mode: "all_in", fraction: 1 },
  fees = { perTradePct: 0.001, slippagePct: 0.0002 },
  execution = "next_close",
  initialCapital = 10000,
}) {
  const rows = normalizeSeries(series);
  if (!rows.length) {
    return { timeline: [], trades: [], fees: { totalFeesPaid: 0, perTradePct: 0, slippagePct: 0 } };
  }

  const equityTimeline = [];
  const trades = [];
  let cashGross = initialCapital;
  let cashNet = initialCapital;
  let sharesGross = 0;
  let sharesNet = 0;
  let inPosition = false;
  let entryPrice = 0;
  let entryPriceGross = 0;
  let entryDate = rows[0].date;
  let entryIndex = 0;
  let maxSinceEntry = 0;
  let minSinceEntry = Infinity;
  let totalFees = 0;
  let pendingAction = null;
  let exitBelowCount = 0;

  const feeRate = fees?.perTradePct ?? 0;
  const slippage = fees?.slippagePct ?? 0;
  const sizeFraction = sizing?.mode === "fixed_frac" ? sizing.fraction ?? 1 : 1;

  const executeTrade = (type, idx, reason) => {
    const bar = rows[idx];
    if (!bar) return;
    const priceGross = bar.close;
    const priceNet = type === "buy" ? bar.close * (1 + slippage) : bar.close * (1 - slippage);

    if (type === "buy" && !inPosition) {
      const allocationGross = cashGross * sizeFraction;
      const allocationNet = cashNet * sizeFraction;
      if (allocationNet <= 0 || allocationGross <= 0) return;
      sharesGross = allocationGross / priceGross;
      sharesNet = allocationNet / priceNet;
      cashGross -= allocationGross;
      const fee = cashNet * feeRate;
      cashNet = cashNet - allocationNet - fee;
      totalFees += fee;
      inPosition = true;
      entryPrice = priceNet;
      entryPriceGross = priceGross;
      entryDate = bar.date;
      entryIndex = idx;
      maxSinceEntry = priceGross;
      minSinceEntry = priceGross;
      exitBelowCount = 0;
    }

    if (type === "sell" && inPosition) {
      cashGross += sharesGross * priceGross;
      const fee = cashNet * feeRate;
      cashNet += sharesNet * priceNet - fee;
      totalFees += fee;
      const grossReturnPct = entryPriceGross ? (priceGross / entryPriceGross - 1) * sizeFraction : 0;
      const netReturnPct = entryPrice ? (priceNet / entryPrice - 1) * sizeFraction : 0;
      const holdingDays = idx - entryIndex;
      trades.push({
        entryDate,
        entryPrice: entryPriceGross,
        exitDate: bar.date,
        exitPrice: priceNet,
        sizePct: sizeFraction,
        grossReturnPct,
        netReturnPct,
        holdingDays,
        exitReason: reason,
        peakSinceEntry: maxSinceEntry,
        troughSinceEntry: minSinceEntry,
      });
      inPosition = false;
      sharesGross = 0;
      sharesNet = 0;
      entryPrice = 0;
      entryPriceGross = 0;
      entryIndex = 0;
      exitBelowCount = 0;
    }
  };

  for (let i = 0; i < rows.length; i++) {
    // Execute any pending action scheduled for this bar
    if (pendingAction && pendingAction.executeIdx === i) {
      executeTrade(pendingAction.type, i, pendingAction.reason);
      pendingAction = null;
    }

    const bar = rows[i];
    const equityGross = cashGross + sharesGross * bar.close;
    const equityNet = cashNet + sharesNet * bar.close;
    equityTimeline.push({
      date: bar.date,
      close: bar.close,
      equityGross,
      equityNet,
      position: inPosition ? 1 : 0,
      pUp: bar.pUp,
      confidence: bar.confidence,
    });

    if (inPosition) {
      maxSinceEntry = Math.max(maxSinceEntry, bar.close);
      minSinceEntry = Math.min(minSinceEntry, bar.close);
    }

    const ctx = {
      i,
      date: bar.date,
      price: bar.close,
      pUp: bar.pUp,
      confidence: bar.confidence,
      position: inPosition ? 1 : 0,
      entryPrice,
      daysInTrade: inPosition ? i - entryIndex : 0,
      peakSinceEntry: maxSinceEntry,
      drawdownSinceEntry: inPosition && maxSinceEntry ? (bar.close - maxSinceEntry) / maxSinceEntry : 0,
    };

    if (inPosition) {
      const exitCheck = exitRule ? exitRule(ctx) : false;
      const stopLoss = stops?.stopLossPct
        ? (bar.close - entryPriceGross) / entryPriceGross <= -Math.abs(stops.stopLossPct)
        : false;
      const trailingStop = stops?.trailingStopPct
        ? maxSinceEntry && (bar.close - maxSinceEntry) / maxSinceEntry <= -Math.abs(stops.trailingStopPct)
        : false;
      const timeStop = stops?.timeStopDays ? ctx.daysInTrade >= stops.timeStopDays : false;

      if (stops?.exitConfirmDays) {
        exitBelowCount = bar.pUp !== null && bar.pUp <= stops.exitThreshold ? exitBelowCount + 1 : 0;
      }

      const confirmExit = stops?.exitConfirmDays
        ? exitBelowCount >= stops.exitConfirmDays && bar.pUp !== null && bar.pUp <= stops.exitThreshold
        : false;

      const shouldExit = exitCheck || stopLoss || trailingStop || timeStop || confirmExit;
      const reason = stopLoss
        ? "STOP_LOSS"
        : trailingStop
          ? "TRAILING_STOP"
          : timeStop
            ? "TIME_STOP"
            : confirmExit
              ? "PUP_DROP"
              : "CUSTOM_EXIT";

      if (shouldExit && i + 1 < rows.length && execution === "next_close") {
        pendingAction = { type: "sell", executeIdx: i + 1, reason };
      } else if (shouldExit) {
        executeTrade("sell", i, reason);
      }
    } else {
      const shouldEnter = entryRule ? entryRule(ctx) : false;
      if (shouldEnter && i + 1 < rows.length && execution === "next_close") {
        pendingAction = { type: "buy", executeIdx: i + 1, reason: "SIGNAL" };
      } else if (shouldEnter) {
        executeTrade("buy", i, "SIGNAL");
      }
    }
  }

  if (pendingAction) {
    executeTrade(pendingAction.type, rows.length - 1, pendingAction.reason || "END" );
  }

  if (inPosition) {
    executeTrade("sell", rows.length - 1, "FORCED_EXIT_END_OF_DATA");
  }

  const paramsUsed = { stops, sizing, fees, execution, initialCapital };
  const netSeries = equityTimeline.map((r) => r.equityNet);
  const grossSeries = equityTimeline.map((r) => r.equityGross);
  const startDate = rows[0].date;
  const endDate = rows[rows.length - 1].date;

  const kpis = summarizeKpis({ timeline: equityTimeline, trades, startDate, endDate });
  const buyHoldStart = rows[0].close;
  const buyHoldEnd = rows[rows.length - 1].close;
  const buyHoldTotal = buyHoldStart ? buyHoldEnd / buyHoldStart - 1 : 0;
  const buyHoldCagr = calcCagrFrom(startDate, endDate, buyHoldStart, buyHoldEnd);
  const buyHoldCurve = rows.map((r, idx) => ({ date: r.date, equity: buyHoldStart ? r.close / buyHoldStart * initialCapital : initialCapital }));

  return {
    paramsUsed,
    timeline: equityTimeline,
    trades,
    kpis: {
      ...kpis,
      totalReturnPct: kpis.totalReturnPct,
    },
    benchmark: {
      buyHoldTotalReturnPct: buyHoldTotal,
      buyHoldCagrPct: buyHoldCagr,
      buyHoldMaxDrawdownPct: calcMaxDrawdown(buyHoldCurve.map((r) => r.equity)),
    },
    fees: {
      totalFeesPaid: totalFees,
      slippageCost: slippage,
      perTradePct: feeRate,
      slippagePct: slippage,
    },
    equityCurves: {
      strategyGross: grossSeries,
      strategyNet: netSeries,
      buyHold: buyHoldCurve.map((r) => r.equity),
    },
  };
}

export function buildStrategyFromUI(params) {
  const entryThreshold = Number(params.entryThreshold ?? 0.6);
  const exitThreshold = Number(params.exitThreshold ?? 0.5);
  const minConfidence = String(params.minConfidence || "MEDIUM").toUpperCase();
  const maxHoldDays = Number(params.maxHoldDays ?? 20);
  const exitConfirmDays = Number(params.exitConfirmDays ?? 0);
  const stopLossPct = params.stopLossPct ? Number(params.stopLossPct) / 100 : null;
  const trailingStopPct = params.trailingStopPct ? Number(params.trailingStopPct) / 100 : null;

  const entryRule = (ctx) => {
    const meetsProb = typeof ctx.pUp === "number" && ctx.pUp >= entryThreshold;
    const meetsConf = confidenceRank(ctx.confidence) >= confidenceRank(minConfidence);
    return !ctx.position && meetsProb && meetsConf;
  };

  const exitRule = (ctx) => {
    const meetsProb = typeof ctx.pUp === "number" && ctx.pUp <= exitThreshold;
    const meetsTime = maxHoldDays ? ctx.daysInTrade >= maxHoldDays : false;
    return ctx.position && (meetsProb || meetsTime);
  };

  const stops = {
    stopLossPct,
    trailingStopPct,
    timeStopDays: maxHoldDays,
    exitConfirmDays,
    exitThreshold,
  };

  const sizing = {
    mode: params.sizingMode || "all_in",
    fraction: params.sizingFraction ?? 1,
  };

  const fees = {
    perTradePct: (Number(params.perTradePct) || 0) / 100,
    slippagePct: (Number(params.slippagePct) || 0) / 100,
  };

  return { entryRule, exitRule, stops, sizing, fees, execution: params.execution || "next_close" };
}

export function runTradingSimulation(params, series) {
  const strategy = buildStrategyFromUI(params);
  const initialCapital = Number(params.initialCapital) || 10000;
  const engineResult = trade_engine_cycle_basic({
    series,
    ...strategy,
    initialCapital,
  });

  const response = {
    ...engineResult,
    paramsUsed: {
      ...engineResult.paramsUsed,
      entryThreshold: params.entryThreshold,
      exitThreshold: params.exitThreshold,
      minConfidence: params.minConfidence,
      initialCapital,
    },
  };

  return response;
}

function calcCagrFrom(startDate, endDate, startPrice, endPrice) {
  const years = yearFraction(startDate, endDate);
  if (!startPrice || !endPrice || !years) return 0;
  return (endPrice / startPrice) ** (1 / years) - 1;
}

function yearFraction(start, end) {
  const s = new Date(start);
  const e = new Date(end);
  const diff = Math.max(1, (e - s) / (1000 * 60 * 60 * 24));
  return diff / 365.25;
}
