// src/lib/assetPaths.js

/**
 * GitHub Pages can be served either from:
 * - https://<user>.github.io/                (root)
 * - https://<user>.github.io/<repo>/         (repo subpath)
 *
 * Also: sometimes data files are committed under:
 * - /data/...
 * - /public/data/...
 *
 * We build a list of candidate URLs to try, in order.
 */

function normalizeSlashes(s) {
  return s.replace(/\/{2,}/g, "/");
}

function getRepoBaseFromLocation() {
  // For user sites: pathname might be "/" or "/something/..."
  // For project pages: pathname starts with "/<repo>/..."
  // We'll assume first segment is repo when not empty and when it matches typical GH pages structure.
  const path = window.location.pathname || "/";
  const segments = path.split("/").filter(Boolean);

  // When a meta tag is provided, honor it before guessing from the URL structure.
  // This ensures custom domains (where pathname is just "/") can still point at
  // a nested repo base like "/WhiteMetal-State/".
  const meta = document.querySelector('meta[name="wm-base"]');
  if (meta?.content) {
    const forced = meta.content.trim();
    if (forced) return forced.endsWith("/") ? forced : `${forced}/`;
  }

  // If you're using https://refaelsheffer.github.io (user site), segments[0] is probably nothing.
  // If you're using https://refaelsheffer.github.io/WhiteMetal-State/, segments[0] === "WhiteMetal-State"
  if (segments.length === 0) return "/";

  // Otherwise: treat first segment as repo base (works for project pages).
  return `/${segments[0]}/`;
}

export function buildCandidateUrls(relativePath) {
  const rel = relativePath.replace(/^\/+/, ""); // no leading slash
  const origin = window.location.origin;

  const repoBase = getRepoBaseFromLocation(); // "/" or "/<repo>/"
  const repoBaseNorm = repoBase.endsWith("/") ? repoBase : repoBase + "/";

  // The most useful candidates for GH Pages:
  const candidates = [
    // 1) repo base + data
    `${origin}${repoBaseNorm}data/${rel}`,
    `${origin}${repoBaseNorm}public/data/${rel}`,

    // 2) root + data (in case user site or you actually deployed to root)
    `${origin}/data/${rel}`,
    `${origin}/public/data/${rel}`,

    // 3) relative (works if you run locally / or base tag is set)
    `data/${rel}`,
    `public/data/${rel}`,
    `./data/${rel}`,
    `./public/data/${rel}`,
  ];

  // Normalize and dedupe
  const uniq = [];
  const seen = new Set();
  for (const u of candidates) {
    const nu = normalizeSlashes(u);
    if (!seen.has(nu)) {
      seen.add(nu);
      uniq.push(nu);
    }
  }
  return uniq;
}
