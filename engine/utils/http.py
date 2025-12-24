from __future__ import annotations

import random
import time
from typing import Any

import requests


def get_with_retry(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    timeout: int = 60,
    max_attempts: int = 4,
    backoff_base: float = 1.0,
    backoff_factor: float = 2.0,
    jitter: float = 0.5,
) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except Exception as exc:  # noqa: PERF203
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_for = backoff_base * (backoff_factor ** (attempt - 1))
            sleep_for += random.uniform(0, jitter)
            time.sleep(sleep_for)
    raise last_exc or RuntimeError(f"Failed to fetch {url}")
