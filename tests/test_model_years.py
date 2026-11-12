import pandas as pd

from ispypsa.model_years import (
    _last_instant_of_interval,
    _model_year_of,
    _model_year_start,
)


def test_model_year_start_is_the_financial_or_calendar_year_boundary():
    assert _model_year_start("fy", 2030) == pd.Timestamp("2029-07-01")
    assert _model_year_start("calendar", 2030) == pd.Timestamp("2030-01-01")


def test_model_year_of_is_the_inverse_of_model_year_start():
    timestamps = pd.Series(
        pd.to_datetime(["2029-06-30 23:00", "2029-07-01 00:00", "2030-06-30 23:00"])
    )

    financial_years = _model_year_of("fy", timestamps)
    calendar_years = _model_year_of("calendar", timestamps)

    pd.testing.assert_series_equal(
        financial_years, pd.Series([2029, 2030, 2030], dtype="int64"), check_names=False
    )
    pd.testing.assert_series_equal(
        calendar_years, pd.Series([2029, 2029, 2030], dtype="int64"), check_names=False
    )


def test_last_instant_of_interval_is_one_second_before_the_stamp():
    snapshots = pd.Series(pd.to_datetime(["2029-07-01 00:00", "2029-07-01 00:30"]))

    last_instants = _last_instant_of_interval(snapshots)

    expected = pd.Series(pd.to_datetime(["2029-06-30 23:59:59", "2029-07-01 00:29:59"]))
    pd.testing.assert_series_equal(last_instants, expected)


def test_snapshot_stamped_on_a_year_boundary_belongs_to_the_year_just_ended():
    snapshots = pd.Series(
        pd.to_datetime(["2029-07-01 00:00", "2029-07-01 00:30", "2030-01-01 00:00"])
    )

    last_instants = _last_instant_of_interval(snapshots)
    financial_years = _model_year_of("fy", last_instants)
    calendar_years = _model_year_of("calendar", last_instants)

    pd.testing.assert_series_equal(
        financial_years, pd.Series([2029, 2030, 2030], dtype="int64"), check_names=False
    )
    pd.testing.assert_series_equal(
        calendar_years, pd.Series([2029, 2029, 2029], dtype="int64"), check_names=False
    )
