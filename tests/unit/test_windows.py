from datetime import datetime, timezone

from src.streaming.windows.sliding_windows import SlidingWindowConfig
from src.streaming.windows.window_utils import (
    calculate_window_start,
    generate_window_starts,
)


def test_sliding_window_configuration_has_valid_interval():
    config = SlidingWindowConfig(
        window_size_seconds=60,
        slide_seconds=10,
    )

    assert config.window_size_seconds == 60
    assert config.slide_seconds == 10


def test_calculate_window_start_aligns_timestamp_to_slide():
    timestamp = datetime(
        2026,
        9,
        12,
        22,
        0,
        37,
        tzinfo=timezone.utc,
    )

    window_start = calculate_window_start(
        timestamp,
        slide_seconds=10,
    )

    assert window_start == datetime(
        2026,
        9,
        12,
        22,
        0,
        30,
        tzinfo=timezone.utc,
    )


def test_generate_window_starts_creates_sliding_windows():
    timestamp = datetime(
        2026,
        9,
        12,
        22,
        1,
        0,
        tzinfo=timezone.utc,
    )

    windows = generate_window_starts(
        timestamp,
        window_size_seconds=60,
        slide_seconds=10,
    )

    assert len(windows) == 6
    assert windows[0] == datetime(
        2026,
        9,
        12,
        22,
        0,
        10,
        tzinfo=timezone.utc,
    )
    assert windows[-1] == timestamp


def test_sliding_window_supports_multiple_overlapping_windows():
    timestamp = datetime(
        2026,
        9,
        12,
        22,
        5,
        0,
        tzinfo=timezone.utc,
    )

    windows = generate_window_starts(
        timestamp,
        window_size_seconds=60,
        slide_seconds=10,
    )

    assert len(windows) == 6
    assert all(
        windows[index] < windows[index + 1]
        for index in range(len(windows) - 1)
    )