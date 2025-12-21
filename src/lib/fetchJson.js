// src/lib/fetchJson.js
import { buildCandidateUrls } from "./assetPaths.js";

export class FetchAllCandidatesError extends Error {
  constructor(message, details) {
    super(message);
    this.name = "FetchAllCandidatesError";
    this.details = details; // array of { url, status, errorText }
  }
}

async function safeReadText(res) {
  try { return await res.text(); } catch { return ""; }
}

export async function fetchJsonWithFallbacks(relativePath, options = {}) {
  const {
    timeoutMs = 12000,
    cache = "no-store",
    debugLabel = relativePath
  } = options;

  const candidates = buildCandidateUrls(relativePath);
  const attempts = [];

  for (const url of candidates) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(url, {
        method: "GET",
        cache,
        headers: { "Accept": "application/json" },
        signal: controller.signal
      });

      clearTimeout(timer);

      if (!res.ok) {
        const t = await safeReadText(res);
        attempts.push({ url, status: res.status, errorText: t.slice(0, 400) });
        continue;
      }

      // Some GH Pages setups return HTML (404 page) with 200 sometimes (rare).
      const contentType = res.headers.get("content-type") || "";
      if (!contentType.includes("application/json")) {
        const t = await safeReadText(res);
        // Try parse anyway, but record a clue.
        try {
          const parsed = JSON.parse(t);
          console.info(`[data] Loaded (non-json header) ${debugLabel} from:`, url);
          return { data: parsed, url };
        } catch {
          attempts.push({ url, status: res.status, errorText: `Non-JSON content-type: ${contentType}. Body head: ${t.slice(0, 200)}` });
          continue;
        }
      }

      const data = await res.json();
      console.info(`[data] Loaded ${debugLabel} from:`, url);
      return { data, url };

    } catch (err) {
      clearTimeout(timer);
      attempts.push({ url, status: "ERR", errorText: String(err?.message || err) });
    }
  }

  console.error(`[data] FAILED to load ${debugLabel}. Attempts:`, attempts);
  throw new FetchAllCandidatesError(`Failed to fetch ${debugLabel} from all candidate URLs`, attempts);
}
