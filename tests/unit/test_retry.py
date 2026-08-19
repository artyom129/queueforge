import pytest

from app.services.retry import exponential_backoff


def test_exponential_backoff_without_jitter() -> None:
    assert exponential_backoff(
        0, base_delay=2, max_delay=60, jitter_ratio=0
    ) == 2
    assert exponential_backoff(
        3, base_delay=2, max_delay=60, jitter_ratio=0
    ) == 16
    assert exponential_backoff(
        20, base_delay=2, max_delay=60, jitter_ratio=0
    ) == 60


def test_jitter_is_bounded() -> None:
    low = exponential_backoff(
        2, base_delay=1, max_delay=60, jitter_ratio=0.1, random_value=0
    )
    high = exponential_backoff(
        2, base_delay=1, max_delay=60, jitter_ratio=0.1, random_value=1
    )
    assert low == pytest.approx(3.6)
    assert high == pytest.approx(4.4)
