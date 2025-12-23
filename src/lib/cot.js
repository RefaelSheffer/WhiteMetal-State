// src/lib/cot.js

function parseDate(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  return Number.isNaN(d.getTime()) ? null : d;
}

export function mapCotHistory(historyPayload) {
  const series = Array.isArray(historyPayload?.series) ? historyPayload.series : [];
  return series
    .map((row) => ({
      as_of: row.as_of || row.date || null,
      commercial_net: row.commercial_net ?? null,
      noncommercial_net: row.noncommercial_net ?? null,
      commercial_net_pct52: row.commercial_net_pct52 ?? row.comm_pct_52 ?? null,
      noncommercial_net_pct52: row.noncommercial_net_pct52 ?? row.nonc_pct_52 ?? null,
      commercial_net_z52: row.commercial_net_z52 ?? null,
      noncommercial_net_z52: row.noncommercial_net_z52 ?? null,
      open_interest: row.open_interest ?? null,
    }))
    .filter((r) => r.as_of)
    .sort((a, b) => (a.as_of > b.as_of ? 1 : -1));
}

export function injectCotIntoDaily(probabilityDaily, cotHistory) {
  if (!Array.isArray(probabilityDaily)) return [];
  const cotSeries = mapCotHistory(cotHistory);
  if (!cotSeries.length) return probabilityDaily;

  const cotDates = cotSeries.map((row) => ({
    date: parseDate(row.as_of),
    row,
  }));

  return probabilityDaily.map((day) => {
    const date = parseDate(day.date);
    if (!date) return day;
    const matching = [...cotDates].reverse().find((entry) => entry.date && entry.date <= date);
    if (!matching) return day;
    const { row } = matching;
    return {
      ...day,
      cotBias: deriveBias(row),
      cotCommercialNetPct52: row.commercial_net_pct52 ?? null,
      cotNoncommercialNetPct52: row.noncommercial_net_pct52 ?? null,
      cotCommercialNetZ52: row.commercial_net_z52 ?? null,
      cotNoncommercialNetZ52: row.noncommercial_net_z52 ?? null,
      cotOpenInterest: row.open_interest ?? null,
    };
  });
}

export function deriveBias(row) {
  const commPct = row?.commercial_net_pct52;
  const noncPct = row?.noncommercial_net_pct52;
  if (commPct !== null && commPct !== undefined && commPct <= 0.1) return "bullish";
  if (noncPct !== null && noncPct !== undefined && noncPct >= 0.9) return "bearish";
  if (noncPct !== null && noncPct !== undefined && noncPct <= 0.1) return "bullish";
  if (commPct !== null && commPct !== undefined && commPct >= 0.9) return "bearish";
  return "neutral";
}

export function describeBias(bias) {
  if (bias === "bullish") return { label: "Bullish", tone: "positive" };
  if (bias === "bearish") return { label: "Bearish", tone: "negative" };
  return { label: "Neutral", tone: "muted" };
}
