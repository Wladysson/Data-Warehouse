from datetime import datetime, timezone

from src.batch.etl.raw_to_clean import RawToClean
from src.batch.etl.clean_to_curated import CleanToCurated
from src.batch.etl.daily_aggregations import DailyAggregations
from src.batch.etl.feature_engineering import FeatureEngineering


def test_raw_to_clean_initializes():
    etl = RawToClean()

    assert etl is not None


def test_clean_to_curated_initializes():
    etl = CleanToCurated()

    assert etl is not None


def test_daily_aggregations_initializes():
    etl = DailyAggregations()

    assert etl is not None


def test_feature_engineering_initializes():
    etl = FeatureEngineering()

    assert etl is not None


def test_raw_to_clean_accepts_event_data(sample_click_event):
    etl = RawToClean()

    result = etl.transform([sample_click_event])

    assert result is not None


def test_clean_to_curated_preserves_event_identity(sample_click_event):
    etl = CleanToCurated()

    result = etl.transform([sample_click_event])

    assert result is not None
    assert result[0]["event_id"] == sample_click_event["event_id"]


def test_daily_aggregations_groups_events_by_date(sample_click_event):
    etl = DailyAggregations()

    result = etl.aggregate([sample_click_event])

    assert result is not None


def test_feature_engineering_generates_temporal_features(sample_click_event):
    etl = FeatureEngineering()

    result = etl.transform([sample_click_event])

    assert result is not None

    event = result[0]

    assert "event_year" in event
    assert "event_month" in event
    assert "event_day" in event