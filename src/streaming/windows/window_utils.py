from __future__ import annotations

from datetime import datetime, timedelta
from typing import List


def validate_window_configuration(
    size_seconds: int,
    slide_seconds: int,
) -> None:

    if size_seconds <= 0:
        raise ValueError(
            "size_seconds deve ser maior que zero."
        )

    if slide_seconds <= 0:
        raise ValueError(
            "slide_seconds deve ser maior que zero."
        )

    if slide_seconds > size_seconds:
        raise ValueError(
            "slide_seconds não pode ser maior que size_seconds."
        )


def floor_timestamp(
    timestamp: datetime,
    interval_seconds: int,
) -> datetime:

    if timestamp.tzinfo is None:
        raise ValueError(
            "timestamp deve possuir timezone."
        )

    if interval_seconds <= 0:
        raise ValueError(
            "interval_seconds deve ser maior que zero."
        )

    epoch = datetime(
        1970,
        1,
        1,
        tzinfo=timestamp.tzinfo,
    )

    elapsed_seconds = (
        timestamp - epoch
    ).total_seconds()

    floored_seconds = (
        int(elapsed_seconds)
        // interval_seconds
    ) * interval_seconds

    return epoch + timedelta(
        seconds=floored_seconds
    )


def calculate_window_start(
    event_timestamp: datetime,
    window_size_seconds: int,
    window_slide_seconds: int,
) -> datetime:

    validate_window_configuration(
        window_size_seconds,
        window_slide_seconds,
    )

    if event_timestamp.tzinfo is None:
        raise ValueError(
            "event_timestamp deve possuir timezone."
        )

    aligned_timestamp = floor_timestamp(
        event_timestamp,
        window_slide_seconds,
    )

    epoch = datetime(
        1970,
        1,
        1,
        tzinfo=event_timestamp.tzinfo,
    )

    elapsed_slide_intervals = int(
        (
            aligned_timestamp - epoch
        ).total_seconds()
        // window_slide_seconds
    )

    windows_per_event = (
        window_size_seconds
        // window_slide_seconds
    )

    latest_start_index = (
        elapsed_slide_intervals
    )

    earliest_start_index = max(
        0,
        latest_start_index - windows_per_event + 1,
    )

    return epoch + timedelta(
        seconds=(
            earliest_start_index
            * window_slide_seconds
        )
    )


def generate_window_starts(
    event_timestamp: datetime,
    window_size_seconds: int,
    window_slide_seconds: int,
) -> List[datetime]:

    validate_window_configuration(
        window_size_seconds,
        window_slide_seconds,
    )

    if event_timestamp.tzinfo is None:
        raise ValueError(
            "event_timestamp deve possuir timezone."
        )

    latest_start = floor_timestamp(
        event_timestamp,
        window_slide_seconds,
    )

    windows_per_event = (
        window_size_seconds
        // window_slide_seconds
    )

    starts: List[datetime] = []

    for index in range(windows_per_event):
        start = (
            latest_start
            - timedelta(
                seconds=(
                    index
                    * window_slide_seconds
                )
            )
        )

        window_end = (
            start
            + timedelta(
                seconds=window_size_seconds
            )
        )

        if (
            start <= event_timestamp
            < window_end
        ):
            starts.append(start)

    return sorted(starts)


def calculate_window_end(
    window_start: datetime,
    window_size_seconds: int,
) -> datetime:

    if window_start.tzinfo is None:
        raise ValueError(
            "window_start deve possuir timezone."
        )

    if window_size_seconds <= 0:
        raise ValueError(
            "window_size_seconds deve ser maior que zero."
        )

    return (
        window_start
        + timedelta(
            seconds=window_size_seconds
        )
    )


def is_event_in_window(
    event_timestamp: datetime,
    window_start: datetime,
    window_size_seconds: int,
) -> bool:

    if event_timestamp.tzinfo is None:
        raise ValueError(
            "event_timestamp deve possuir timezone."
        )

    window_end = calculate_window_end(
        window_start,
        window_size_seconds,
    )

    return (
        window_start
        <= event_timestamp
        < window_end
    )