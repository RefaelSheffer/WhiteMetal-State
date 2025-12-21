// src/lib/data-hub.js
import { fetchJsonWithFallbacks } from "./fetchJson.js";

function assertMeta(meta) {
  if (!meta || typeof meta !== "object") throw new Error("meta.json is not an object");
  if (!meta.files || typeof meta.files !== "object") {
    throw new Error("meta.json missing 'files' object (mapping dataset keys -> filenames)");
  }
  return true;
}

export function createDataHub() {
  const state = {
    status: "idle",
    progress: { total: 0, done: 0 },
    meta: null,
    datasets: {},
    loadedFrom: { metaUrl: null, files: {} },
    errors: {},
  };

  const listeners = new Set();
  const emit = () => listeners.forEach((fn) => fn(structuredClone(state)));

  function subscribe(fn) {
    listeners.add(fn);
    fn(structuredClone(state));
    return () => listeners.delete(fn);
  }

  function getMeta() { return state.meta; }
  function getDataset(key) { return state.datasets[key]; }

  async function loadAll() {
    state.status = "loading";
    state.progress = { total: 0, done: 0 };
    state.errors = {};
    emit();

    let metaRes;
    try {
      metaRes = await fetchJsonWithFallbacks("meta.json", { debugLabel: "meta.json" });
      state.meta = metaRes.data;
      state.loadedFrom.metaUrl = metaRes.url;
      assertMeta(state.meta);
      emit();
    } catch (e) {
      state.status = "error";
      state.errors.meta = { message: e.message, details: e.details };
      emit();
      throw e;
    }

    const entries = Object.entries(state.meta.files);
    state.progress.total = entries.length;
    emit();

    const tasks = entries.map(async ([key, filename]) => {
      try {
        const res = await fetchJsonWithFallbacks(filename, { debugLabel: filename });
        state.datasets[key] = res.data;
        state.loadedFrom.files[key] = res.url;
      } catch (e) {
        state.errors[key] = { message: e.message, details: e.details };
      } finally {
        state.progress.done += 1;
        emit();
      }
    });

    await Promise.all(tasks);

    const failedKeys = Object.keys(state.errors).filter((k) => k !== "meta");
    if (failedKeys.length > 0) {
      state.status = "ready";
      emit();
      return { ok: false, failedKeys };
    }

    state.status = "ready";
    emit();
    return { ok: true };
  }

  return { subscribe, loadAll, getMeta, getDataset };
}
