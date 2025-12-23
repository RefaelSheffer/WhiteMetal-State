import json
import math
from pathlib import Path
from typing import Iterable, Mapping, Union

try:  # Optional dependency in some environments
    import numpy as np
except Exception:  # pragma: no cover - numpy may not be installed
    np = None

PathLike = Union[str, Path]


def _is_nan_like(value) -> bool:
    if isinstance(value, float) and math.isnan(value):
        return True
    if np is not None and isinstance(value, np.floating):
        return math.isnan(float(value))
    return False


def _is_infinite(value) -> bool:
    if isinstance(value, (float, int)):
        return math.isinf(value)
    if np is not None and isinstance(value, np.floating):
        return math.isinf(float(value))
    return False


def sanitize_for_json(obj):
    """Recursively clean objects before JSON serialization.

    * NaN/inf/-inf become ``None``
    * Strings like "NaN"/"nan"/""/"null" become ``None``
    * numpy scalars are coerced to Python numbers before checks
    """

    if obj is None:
        return None

    if isinstance(obj, Mapping):
        return {k: sanitize_for_json(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set)):
        return [sanitize_for_json(v) for v in obj]

    if isinstance(obj, bool):
        return obj

    if isinstance(obj, str):
        trimmed = obj.strip()
        if trimmed.lower() in {"nan", "", "null"}:
            return None
        return trimmed

    if np is not None and isinstance(obj, np.generic):
        obj = obj.item()

    if isinstance(obj, (int, float)):
        if _is_nan_like(obj) or _is_infinite(obj):
            return None
        return float(obj) if isinstance(obj, float) else int(obj)

    return obj


def ensure_parent(path: PathLike) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: PathLike, data: Mapping) -> None:
    path_obj = ensure_parent(path)
    path_obj.write_text(json.dumps(sanitize_for_json(data), indent=2))


def write_jsonl(path: PathLike, records: Iterable[Mapping]) -> None:
    path_obj = ensure_parent(path)
    lines = [json.dumps(sanitize_for_json(record)) for record in records]
    path_obj.write_text("\n".join(lines))
