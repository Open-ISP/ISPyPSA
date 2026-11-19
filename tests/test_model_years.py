import pandas as pd

from ispypsa.model_years import (
    _last_instant_of_interval,
    _model_year_of_dates,
    _model_year_of_interval_ends,
    _model_year_start,
)


def test_model_year_start_is_the_financial_or_calendar_year_boundary():
    assert _model_year_start("fy", 2030) == pd.Timestamp("2029-07-01")
    assert _model_year_start("calendar", 2030) == pd.Timestamp("2030-01-01")


def test_model_year_of_dates_is_the_inverse_of_model_year_start():
    financial_year_start = pd.Series([_model_year_start("fy", 2030)])
    calendar_year_start = pd.Series([_model_year_start("calendar", 2030)])

    financial_years = _model_year_of_dates("fy", financial_year_start)
    calendar_years = _model_year_of_dates("calendar", calendar_year_start)

    pd.testing.assert_series_equal(financial_years, pd.Series([2030], dtype="int64"))
    pd.testing.assert_series_equal(calendar_years, pd.Series([2030], dtype="int64"))


def test_date_on_a_year_boundary_belongs_to_the_year_starting_that_day():
    # The last and first day either side of each boundary: 30 June / 1 July
    # for financial years, 31 December / 1 January for calendar years.
    dates = pd.Series(
        pd.to_datetime(["2029-06-30", "2029-07-01", "2029-12-31", "2030-01-01"])
    )

    financial_years = _model_year_of_dates("fy", dates)
    calendar_years = _model_year_of_dates("calendar", dates)

    pd.testing.assert_series_equal(
        financial_years, pd.Series([2029, 2030, 2030, 2030], dtype="int64")
    )
    pd.testing.assert_series_equal(
        calendar_years, pd.Series([2029, 2029, 2029, 2030], dtype="int64")
    )


def test_last_instant_of_interval_is_one_second_before_the_stamp():
    snapshots = pd.Series(pd.to_datetime(["2029-07-01 00:00", "2029-07-01 00:30"]))

    last_instants = _last_instant_of_interval(snapshots)

    expected = pd.Series(pd.to_datetime(["2029-06-30 23:59:59", "2029-07-01 00:29:59"]))
    pd.testing.assert_series_equal(last_instants, expected)


def test_interval_ending_on_a_year_boundary_belongs_to_the_year_just_ended():
    # The interval ending at midnight on each boundary, and the one after it.
    interval_ends = pd.Series(
        pd.to_datetime(
            [
                "2029-07-01 00:00",
                "2029-07-01 00:30",
                "2030-01-01 00:00",
                "2030-01-01 00:30",
            ]
        )
    )

    financial_years = _model_year_of_interval_ends("fy", interval_ends)
    calendar_years = _model_year_of_interval_ends("calendar", interval_ends)

    pd.testing.assert_series_equal(
        financial_years, pd.Series([2029, 2030, 2030, 2030], dtype="int64")
    )
    pd.testing.assert_series_equal(
        calendar_years, pd.Series([2029, 2029, 2029, 2030], dtype="int64")
    )
