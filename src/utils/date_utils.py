from __future__ import annotations

from datetime import (
    date,
    datetime,
    timedelta,
    timezone,
)


class DateUtils:

    @staticmethod
    def now_utc() -> datetime:
        """Retorna o instante atual em UTC."""

        return datetime.now(timezone.utc)

    @staticmethod
    def today_utc() -> date:
        """Retorna a data atual em UTC."""

        return DateUtils.now_utc().date()

    @staticmethod
    def ensure_timezone(
        value: datetime,
        default_timezone=timezone.utc,
    ) -> datetime:

        if not isinstance(value, datetime):
            raise TypeError(
                "value deve ser um datetime."
            )

        if value.tzinfo is None:
            return value.replace(
                tzinfo=default_timezone
            )

        return value

    @staticmethod
    def to_utc(
        value: datetime,
    ) -> datetime:

        aware_value = DateUtils.ensure_timezone(value)

        return aware_value.astimezone(
            timezone.utc
        )

    @staticmethod
    def isoformat(
        value: datetime,
    ) -> str:

        return DateUtils.to_utc(value).isoformat()

    @staticmethod
    def parse_datetime(
        value: str,
    ) -> datetime:

        if not isinstance(value, str):
            raise TypeError(
                "value deve ser uma string."
            )

        normalized = value.strip()

        if not normalized:
            raise ValueError(
                "value não pode ser vazio."
            )

        if normalized.endswith("Z"):
            normalized = (
                normalized[:-1] + "+00:00"
            )

        parsed = datetime.fromisoformat(
            normalized
        )

        return DateUtils.to_utc(parsed)

    @staticmethod
    def parse_date(
        value: str,
    ) -> date:

        if not isinstance(value, str):
            raise TypeError(
                "value deve ser uma string."
            )

        normalized = value.strip()

        if not normalized:
            raise ValueError(
                "value não pode ser vazio."
            )

        return date.fromisoformat(normalized)

    @staticmethod
    def start_of_day(
        value: date | datetime,
    ) -> datetime:

        if isinstance(value, datetime):
            current_date = DateUtils.to_utc(
                value
            ).date()
        elif isinstance(value, date):
            current_date = value
        else:
            raise TypeError(
                "value deve ser date ou datetime."
            )

        return datetime.combine(
            current_date,
            datetime.min.time(),
            tzinfo=timezone.utc,
        )

    @staticmethod
    def end_of_day(
        value: date | datetime,
    ) -> datetime:

        return DateUtils.start_of_day(value) + timedelta(
            days=1
        ) - timedelta(
            microseconds=1
        )

    @staticmethod
    def start_of_hour(
        value: datetime,
    ) -> datetime:

        current = DateUtils.to_utc(value)

        return current.replace(
            minute=0,
            second=0,
            microsecond=0,
        )

    @staticmethod
    def end_of_hour(
        value: datetime,
    ) -> datetime:

        return DateUtils.start_of_hour(value) + timedelta(
            hours=1
        ) - timedelta(
            microseconds=1
        )

    @staticmethod
    def add_seconds(
        value: datetime,
        seconds: float,
    ) -> datetime:

        return DateUtils.to_utc(value) + timedelta(
            seconds=seconds
        )

    @staticmethod
    def add_minutes(
        value: datetime,
        minutes: float,
    ) -> datetime:

        return DateUtils.to_utc(value) + timedelta(
            minutes=minutes
        )

    @staticmethod
    def add_hours(
        value: datetime,
        hours: float,
    ) -> datetime:

        return DateUtils.to_utc(value) + timedelta(
            hours=hours
        )

    @staticmethod
    def add_days(
        value: datetime,
        days: float,
    ) -> datetime:

        return DateUtils.to_utc(value) + timedelta(
            days=days
        )

    @staticmethod
    def difference_seconds(
        start: datetime,
        end: datetime,
    ) -> float:

        start_utc = DateUtils.to_utc(start)
        end_utc = DateUtils.to_utc(end)

        return (
            end_utc - start_utc
        ).total_seconds()

    @staticmethod
    def is_late(
        event_time: datetime,
        ingestion_time: datetime,
    ) -> bool:

        return DateUtils.to_utc(
            ingestion_time
        ) > DateUtils.to_utc(
            event_time
        )

    @staticmethod
    def partition_values(
        value: date | datetime,
    ) -> dict[str, int]:

        if isinstance(value, datetime):
            current = DateUtils.to_utc(value)
        elif isinstance(value, date):
            current = datetime.combine(
                value,
                datetime.min.time(),
                tzinfo=timezone.utc,
            )
        else:
            raise TypeError(
                "value deve ser date ou datetime."
            )

        return {
            "year": current.year,
            "month": current.month,
            "day": current.day,
            "hour": current.hour,
        }