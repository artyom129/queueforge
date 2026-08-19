from __future__ import annotations

import random


def exponential_backoff(
    retry_count: int,
    *,
    base_delay: float,
    max_delay: float,
    jitter_ratio: float,
    random_value: float | None = None,
) -> float:
    raw = min(base_delay * (2**retry_count), max_delay)
    if jitter_ratio <= 0:
        return raw
    unit = random.random() if random_value is None else random_value
    # Symmetric bounded jitter: ± jitter_ratio.
    multiplier = 1.0 + ((unit * 2.0) - 1.0) * jitter_ratio
    return max(0.0, raw * multiplier)
