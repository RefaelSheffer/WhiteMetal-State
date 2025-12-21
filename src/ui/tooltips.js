// src/ui/tooltips.js
let tooltipEl = null;

function ensureTooltipEl() {
  if (tooltipEl) return tooltipEl;
  tooltipEl = document.createElement("div");
  tooltipEl.className = "tooltip-bubble";
  tooltipEl.style.display = "none";
  document.body.appendChild(tooltipEl);
  return tooltipEl;
}

export function attachTooltips(root = document) {
  const scope = root?.querySelectorAll ? root : document;
  const tooltip = ensureTooltipEl();
  const targets = scope.querySelectorAll("[data-tooltip]");
  targets.forEach((el) => {
    if (el.dataset.tooltipBound === "true") return;
    el.dataset.tooltipBound = "true";

    const show = (evt) => {
      tooltip.textContent = el.dataset.tooltip || "";
      tooltip.style.display = "block";
      tooltip.style.left = `${(evt.clientX || 0) + 12}px`;
      tooltip.style.top = `${(evt.clientY || 0) + 12}px`;
    };

    const move = (evt) => {
      tooltip.style.left = `${(evt.clientX || 0) + 12}px`;
      tooltip.style.top = `${(evt.clientY || 0) + 12}px`;
    };

    const hide = () => { tooltip.style.display = "none"; };

    el.addEventListener("mouseenter", show);
    el.addEventListener("mousemove", move);
    el.addEventListener("mouseleave", hide);
    el.addEventListener("focus", (evt) => show(evt));
    el.addEventListener("blur", hide);
  });
}
