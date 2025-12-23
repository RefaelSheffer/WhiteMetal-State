// src/ui/tooltips.js

let tooltipEl = null;
let glossaryPromise = null;
let globalHideBound = false;
let releasePinned = null;

const glossaryLinkLabels = {
  en: "Read more in glossary",
  he: "קרא עוד במילון",
  es: "Leer más en el glosario",
};

function ensureTooltipEl() {
  if (tooltipEl) return tooltipEl;
  tooltipEl = document.createElement("div");
  tooltipEl.className = "tooltip-bubble";
  tooltipEl.style.display = "none";
  document.body.appendChild(tooltipEl);
  return tooltipEl;
}

function hideTooltip() {
  const el = ensureTooltipEl();
  el.style.display = "none";
  if (typeof releasePinned === "function") releasePinned();
  releasePinned = null;
}

function escapeHtml(str) {
  return (str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function pickLang(map, lang) {
  if (!map) return "";
  if (map[lang]) return map[lang];
  if (map.en) return map.en;
  const first = Object.values(map)[0];
  return typeof first === "string" ? first : "";
}

async function loadGlossary() {
  if (!glossaryPromise) {
    glossaryPromise = fetch("./docs/glossary-data.json", { cache: "force-cache" })
      .then((res) => (res.ok ? res.json() : Promise.reject(new Error(`HTTP ${res.status}`))))
      .catch(() => ({ terms: [] }));
  }
  const data = await glossaryPromise;
  return Array.isArray(data?.terms) ? data.terms : [];
}

function resolveGlossaryHref(termId, lang) {
  const safeLang = ["en", "he", "es"].includes(lang) ? lang : "en";
  return `./docs/glossary-${safeLang}.html#${encodeURIComponent(termId)}`;
}

async function glossaryContent(termId, lang) {
  if (!termId) return null;
  const terms = await loadGlossary();
  const term = terms.find((t) => t.id === termId);
  if (!term) return null;
  const title = pickLang(term.title, lang) || term.id;
  const body = pickLang(term.short, lang) || pickLang(term.long, lang) || "";
  const href = resolveGlossaryHref(termId, lang);
  const linkLabel = glossaryLinkLabels[lang] || glossaryLinkLabels.en;
  return `
    <div class="tooltip-title">${escapeHtml(title)}</div>
    <div class="tooltip-body">${escapeHtml(body)}</div>
    <div class="tooltip-link"><a href="${href}" target="_blank" rel="noopener">${escapeHtml(linkLabel)}</a></div>
  `;
}

function positionTooltip(evt, el) {
  const tooltip = ensureTooltipEl();
  const rect = el?.getBoundingClientRect();
  const baseX = evt?.clientX ?? (rect ? rect.left + rect.width / 2 : 0);
  const baseY = evt?.clientY ?? (rect ? rect.top + rect.height / 2 : 0);
  const padding = 12;

  tooltip.style.left = `${baseX + padding}px`;
  tooltip.style.top = `${baseY + padding}px`;

  requestAnimationFrame(() => {
    const { offsetWidth, offsetHeight } = tooltip;
    let left = baseX + padding;
    let top = baseY + padding;
    if (left + offsetWidth + 8 > window.innerWidth) {
      left = Math.max(8, window.innerWidth - offsetWidth - 8);
    }
    if (top + offsetHeight + 8 > window.innerHeight && rect) {
      top = Math.max(8, rect.top - offsetHeight - padding);
    }
    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
  });
}

function bindTooltip(el, lang) {
  const existingCleanup = el.__tooltipCleanup;
  if (existingCleanup && el.dataset.tooltipLang === lang) return;
  if (existingCleanup) existingCleanup();
  let pinned = false;

  const getContent = async () => {
    if (el.dataset.glossary) {
      const html = await glossaryContent(el.dataset.glossary, lang);
      if (html) return html;
    }
    const text = el.dataset.tooltip || "";
    if (!text) return null;
    return `<div class="tooltip-body">${escapeHtml(text)}</div>`;
  };

  const tooltip = ensureTooltipEl();

  const show = async (evt) => {
    const html = await getContent();
    if (!html) return;
    tooltip.innerHTML = html;
    tooltip.style.display = "block";
    positionTooltip(evt, el);
  };

  const move = (evt) => {
    if (tooltip.style.display === "block") {
      positionTooltip(evt, el);
    }
  };

  const hide = () => hideTooltip();

  const showHandler = (evt) => show(evt);
  const moveHandler = (evt) => move(evt);
  const leaveHandler = () => {
    if (!pinned) hide();
  };
  const clickHandler = (evt) => {
    evt.preventDefault();
    evt.stopPropagation();
    if (tooltip.style.display === "block" && pinned) {
      hide();
      return;
    }
    pinned = true;
    releasePinned = () => {
      pinned = false;
    };
    show(evt);
  };

  el.addEventListener("mouseenter", showHandler);
  el.addEventListener("mousemove", moveHandler);
  el.addEventListener("mouseleave", leaveHandler);
  el.addEventListener("focus", showHandler);
  el.addEventListener("blur", leaveHandler);
  el.addEventListener("click", clickHandler);

  el.__tooltipCleanup = () => {
    el.removeEventListener("mouseenter", showHandler);
    el.removeEventListener("mousemove", moveHandler);
    el.removeEventListener("mouseleave", leaveHandler);
    el.removeEventListener("focus", showHandler);
    el.removeEventListener("blur", leaveHandler);
    el.removeEventListener("click", clickHandler);
  };
  el.dataset.tooltipLang = lang;

  if (!globalHideBound) {
    globalHideBound = true;
    document.addEventListener("keydown", (evt) => {
      if (evt.key === "Escape") hideTooltip();
    });
    document.addEventListener("click", (evt) => {
      const bubble = ensureTooltipEl();
      if (!bubble.contains(evt.target)) hideTooltip();
    });
  }
}

export function attachTooltips(root = document, lang = "en") {
  const scope = root?.querySelectorAll ? root : document;
  const targets = scope.querySelectorAll("[data-tooltip], [data-glossary]");
  targets.forEach((el) => bindTooltip(el, lang));
}
