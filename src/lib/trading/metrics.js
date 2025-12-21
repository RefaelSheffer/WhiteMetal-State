// src/lib/trading/metrics.js
export function calcDailyReturns(equitySeries) {
  if (!Array.isArray(equitySeries) || equitySeries.length < 2) return [];
  const returns = [];
  for (let i = 1; i < equitySeries.length; i++) {
    const prev = equitySeries[i - 1];
    const curr = equitySeries[i];
    if (!prev || !curr) continue;
    const prevEq = Number(prev.equityNet ?? prev.equity ?? 0);
    const currEq = Number(curr.equityNet ?? curr.equity ?? 0);
    if (!prevEq) continue;
    returns.push(currEq / prevEq - 1);
  }
  return returns;
}

export function calcMaxDrawdown(values) {
  if (!values?.length) return 0;
  let peak = values[0];
  let maxDd = 0;
  values.forEach((v) => {
    if (v > peak) peak = v;
    if (peak) {
      const dd = (v - peak) / peak;
      if (dd < maxDd) maxDd = dd;
    }
  });
  return maxDd;
}

export function calcCagr(startValue, endValue, years) {
  if (!startValue || !endValue || years <= 0) return 0;
  return (endValue / startValue) ** (1 / years) - 1;
}

export function calcSharpe(returns) {
  if (!returns?.length) return 0;
  const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
  const variance = returns.reduce((acc, r) => acc + (r - mean) ** 2, 0) / returns.length;
  const std = Math.sqrt(variance);
  if (!std) return 0;
  return (mean / std) * Math.sqrt(252);
}

export function calcSortino(returns) {
  if (!returns?.length) return 0;
  const negatives = returns.filter((r) => r < 0);
  if (!negatives.length) return calcSharpe(returns);
  const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
  const variance = negatives.reduce((acc, r) => acc + (r - mean) ** 2, 0) / negatives.length;
  const std = Math.sqrt(variance);
  if (!std) return 0;
  return (mean / std) * Math.sqrt(252);
}

export function calcWinRate(trades) {
  if (!Array.isArray(trades) || !trades.length) return 0;
  const wins = trades.filter((t) => (t.netReturnPct ?? 0) > 0).length;
  return wins / trades.length;
}

export function calcProfitFactor(trades) {
  if (!Array.isArray(trades) || !trades.length) return 0;
  let gains = 0;
  let losses = 0;
  trades.forEach((t) => {
    const ret = t.netReturnPct ?? 0;
    if (ret > 0) gains += ret;
    else losses += ret;
  });
  if (!losses) return gains ? Infinity : 0;
  return Math.abs(gains / losses);
}

export function calcExposure(trades, totalDays) {
  if (!Array.isArray(trades) || !trades.length || !totalDays) return 0;
  const investedDays = trades.reduce((acc, t) => acc + (t.holdingDays ?? 0), 0);
  return investedDays / totalDays;
}

export function summarizeKpis({
  timeline,
  trades,
  startDate,
  endDate,
}) {
  if (!timeline?.length) return {
    totalReturnPct: 0,
    cagrPct: 0,
    maxDrawdownPct: 0,
    sharpe: 0,
    sortino: 0,
    winRatePct: 0,
    profitFactor: 0,
    avgTradePct: 0,
    tradesCount: trades?.length || 0,
    exposurePct: 0,
  };

  const netValues = timeline.map((t) => t.equityNet ?? t.equity ?? 0);
  const start = netValues[0];
  const end = netValues[netValues.length - 1];
  const dailyReturns = calcDailyReturns(timeline.map((t) => ({ equityNet: t.equityNet })));
  const years = startDate && endDate ? yearFraction(startDate, endDate) : 0;
  const cagr = calcCagr(start, end, years);
  const avgTradePct = trades?.length ? trades.reduce((acc, t) => acc + (t.netReturnPct ?? 0), 0) / trades.length : 0;

  return {
    totalReturnPct: end && start ? end / start - 1 : 0,
    cagrPct: cagr,
    maxDrawdownPct: calcMaxDrawdown(netValues),
    sharpe: calcSharpe(dailyReturns),
    sortino: calcSortino(dailyReturns),
    winRatePct: calcWinRate(trades),
    profitFactor: calcProfitFactor(trades),
    avgTradePct,
    tradesCount: trades?.length || 0,
    exposurePct: calcExposure(trades, timeline.length),
  };
}

function yearFraction(start, end) {
  try {
    const s = new Date(start);
    const e = new Date(end);
    const diff = Math.max(1, (e - s) / (1000 * 60 * 60 * 24));
    return diff / 365.25;
  } catch (e) {
    return 0;
  }
}
